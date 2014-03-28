# basically we are porting this https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example

require "java"

require "jruby-kafka/namespace"
require "jruby-kafka/error"

java_import 'kafka.common.FailedToSendMessageException'

class Kafka::Producer
  @topic

  # Create a Kafka Producer
  #
  # options:
  # :topic_id => "topic" - REQUIRED: The topic id to consume on.
  # :broker_list => "localhost:9092" - REQUIRED: a seed list of kafka brokers
  def initialize(options={})
    validate_required_arguments(options)

    @brokers = options[:broker_list] ? options[:broker_list] : "localhost:9092"

    required_acks = ['-1', '0', '1']
    @request_required_acks = options[:request_required_acks] && required_acks.include? options[:request_required_acks] ? options[:request_required_acks] : '0'

    @request_timeout_ms = options[:request_timeout_ms] ? options[:request_timeout_ms] : '10000'
    @producer_type = options[:producer_type] ? options[:producer_type] : 'sync'
    @serializer_class = options[:serializer_class] ? options[:serializer_class] : 'kafka.serializer.StringEncoder'
    @key_serializer_class = options[:key_serializer_class] ? options[:key_serializer_class] : @serializer_class
    @partitioner_class = options[:partitioner_class] ? options[:partitioner_class] : nil

    required_codecs = ["#{Java::kafka::message::NoCompressionCodec.name}",
                         "#{Java::kafka::message::GZIPCompressionCodec.name}",
                         "#{Java::kafka::message::SnappyCompressionCodec.name}"]
    @compression_codec = options[:compression_codec] && required_codecs.include? options[:compression_codec]  ? options[:compression_codec] : "#{Java::kafka::message::NoCompressionCodec.name}"

    @compressed_topics = options[:compressed_topics] ? options[:compressed_topics] : nil
    @message_send_max_retries = options[:message_send_max_retries] ? options[:message_send_max_retries] : '3'
    @retry_backoff_ms = options[:retry_backoff_ms] ? options[:retry_backoff_ms] : '100'
    @topic_metadata_refresh_interval_ms = options[:topic_metadata_refresh_interval_ms] ? options[:topic_metadata_refresh_interval_ms] : '600000' # 600 * 1000
    @queue_buffering_max_ms = options[:queue_buffering_max_ms] ? options[:queue_buffering_max_ms] : '5000'
    @queue_buffering_max_messages = options[:queue_buffering_max_messages] ? options[:queue_buffering_max_messages] : '10000'
    @queue_enqueue_timeout_ms = options[:queue_enqueue_timeout_ms] ? options[:queue_enqueue_timeout_ms] : '-1'
    @batch_num_messages = options[:batch_num_messages] ? options[:batch_num_messages] : '200'
    @send_buffer_bytes = options[:send_buffer_bytes] ? options[:send_buffer_bytes] : '102400' # 100 * 1024
    @client_id = options[:client_id] ? options[:client_id] : ''

  end

  private
  def validate_required_arguments(options={})
    [:broker_list].each do |opt|
      raise(ArgumentError, "#{opt} is required.") unless options[opt]
    end
  end

  public
  def connect()
    @producer = Java::kafka::producer::Producer.new(createProducerConfig)
  end

  public
  def sendMsg(topic, key, msg)
    m = Java::kafka::producer::KeyedMessage.new(topic=topic, key=key, message=msg)
    #the send message for a producer is scala varargs, which doesn't seem to play nice w/ jruby
    #  this is the best I could come up with
    ms = Java::scala::collection::immutable::Vector.new(0,0,0)
    ms = ms.append_front(m)
    begin
      @producer.send(ms)
    rescue FailedToSendMessageException => e
      raise KafkaError.new(e), "Got FailedToSendMessageException: #{e}"
    end
  end

  def createProducerConfig()
    # TODO lots more options avaiable here: http://kafka.apache.org/documentation.html#producerconfigs
    properties = java.util.Properties.new()
    properties.put("metadata.broker.list", @brokers)
    properties.put("request.required.acks", @request_required_acks)
    properties.put("request.timeout.ms", @request_timeout_ms)
    properties.put("producer.type", @producer_type)
    properties.put("serializer.class", @serializer_class)
    properties.put("key.serializer.class", @key_serializer_class)
    properties.put("partitioner.class", @partitioner_class) if not @partitioner_class.nil?
    properties.put("compression.codec", @compression_codec)
    properties.put("compressed.topics", @compressed_topics)
    properties.put("message.send.max.retries", @message_send_max_retries)
    properties.put("retry.backoff.ms", @retry_backoff_ms)
    properties.put("topic.metadata.refresh.interval.ms", @topic_metadata_refresh_interval_ms)
    properties.put("queue.buffering.max.ms", @queue_buffering_max_ms)
    properties.put("queue.buffering.max.messages", @queue_buffering_max_messages)
    properties.put("queue.enqueue.timeout.ms", @queue_enqueue_timeout_ms)
    properties.put("batch.num.messages", @batch_num_messages)
    properties.put("send.buffer.bytes", @send_buffer.bytes)
    properties.put("client.id", @client_id)

    return Java::kafka::producer::ProducerConfig.new(properties)
  end
end
