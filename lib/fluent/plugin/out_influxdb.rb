# encoding: UTF-8
require 'date'
require 'excon'
require 'influxdb'
require 'json'
require 'uri'
begin
  require 'strptime'
rescue LoadError
end

require 'fluent/output'

class Fluent::InfluxdbObjectOutput < Fluent::ObjectBufferedOutput
  class ConnectionFailure < StandardError; end

  Fluent::Plugin.register_output('influxdb_object', self)

  config_param :host, :string,  default: 'localhost'
  config_param :port, :integer,  default: 8086
  config_param :dbname, :string,  default: 'fluentd'
  config_param :measurement, :string, default: nil
  config_param :user, :string,  default: 'root'
  config_param :password, :string,  default: 'root', secret: true
  config_param :retry, :integer, default: nil
  config_param :time_key, :string, default: 'time'
  config_param :time_precision, :string, default: 's'
  config_param :use_ssl, :bool, default: false
  config_param :verify_ssl, :bool, default: true
  config_param :tag_keys, :array, default: []

  def initialize
    super
  end

  def configure(conf)
    super
    log.info "configure"
    @time_parser = create_time_parser

    log.info "Connecting to database: #{@dbname}, host: #{@host}, port: #{@port}, username: #{@user}, use_ssl = #{@use_ssl}, verify_ssl = #{@verify_ssl}"
    @influxdb ||= InfluxDB::Client.new @dbname, hosts: @host.split(','),
                                                port: @port,
                                                username: @user,
                                                password: @password,
                                                async: false,
                                                retry: @retry,
                                                time_precision: @time_precision,
                                                use_ssl: @use_ssl,
                                                verify_ssl: @verify_ssl

    begin
      existing_databases = @influxdb.list_databases.map { |x| x['name'] }
      unless existing_databases.include? @dbname
        raise Fluent::ConfigError, 'Database ' + @dbname + ' doesn\'t exist. Create it first, please. Existing databases: ' + existing_databases.join(',')
      end
      log.info 'OK: Database ' + @dbname + ' exist.'
    rescue InfluxDB::AuthenticationError, InfluxDB::Error
      log.info "skip database presence check because '#{@user}' user doesn't have admin privilege. Check '#{@dbname}' exists on influxdb"
    end

  end

  # once fluent v0.14 is released we might be able to use
  # Fluent::Parser::TimeParser, but it doesn't quite do what we want - if gives
  # [sec,nsec] where as we want something we can call `strftime` on...
  def create_time_parser
    if @time_key_format
      begin
        # Strptime doesn't support all formats, but for those it does it's
        # blazingly fast.
        strptime = Strptime.new(@time_key_format)
        Proc.new { |value| strptime.exec(value).to_datetime }
      rescue
        # Can happen if Strptime doesn't recognize the format; or
        # if strptime couldn't be required (because it's not installed -- it's
        # ruby 2 only)
        Proc.new { |value| DateTime.strptime(value, @time_key_format) }
      end
    else
      Proc.new { |value| DateTime.parse(value) }
    end
  end

  def parse_time(value, event_time, tag)
    @time_parser.call(value)
  rescue => e
    router.emit_error_event(@time_parse_error_tag, Fluent::Engine.now, {'tag' => tag, 'time' => event_time, 'format' => @time_key_format, 'value' => value}, e)
    return Time.at(event_time).to_datetime
  end


  def write_objects(tag, chunk)
    tag = chunk.metadata.tag
    points = []

    chunk.msgpack_each do |time, record|
      next unless record.is_a? Hash
#      timestamp = record.delete(@time_key) || time
#      timestamp = DateTime.strptime(record.delete(@time_key), @time_key_format).to_time.to_i || time
      timestamp =
        if @time_key_format
          DateTime.strptime(record.delete(@time_key), @time_key_format).to_time.to_i || time
        else
          DateTime.parse(record.delete(@time_key)).to_time.to_i || time
        end
      log.info timestamp

      values = {}
      tags = {}

      record.each_pair do |k, v|
        if v.is_a? Numeric then
          values[k] = v
        end
        if @tag_keys.include?(k) then
          tags[k] = v if v.to_s.strip != ''
        end
      end

      if values.empty?
        log.warn "Skip record '#{record}', because InfluxDB requires at least one value in raw"
        next
      end

      point = {
        timestamp: timestamp,
        series: @measurement || tag,
        values: values,
        tags: tags,
      }
      # log.info timestamp

      points << point
    end

    if points.size > 0 then
      log.info "write points size: #{points.size}"
      @influxdb.write_points(points)
    end

  end

end
