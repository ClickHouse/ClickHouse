#include <Common/config.h>
#if USE_RDKAFKA

#include <thread>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <DataStreams/FormatFactory.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/StorageKafka.h>
#include <common/logger_useful.h>

#include <rdkafka.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_READ_FROM_ISTREAM;
}

/// How long to wait for a single message (applies to each individual message)
static const auto READ_POLL_MS       = 1 * 1000;
static const auto CLEANUP_TIMEOUT_MS = 2 * 1000;

/// How many messages to pull out of internal queue at once
static const UInt64 BATCH_SIZE_MAX   = 16;

class ReadBufferFromKafkaConsumer : public ReadBuffer
{
    using Messages = std::vector<rd_kafka_message_t *>;

    rd_kafka_t * consumer;
    Messages messages;
    Messages::iterator current;
    Messages::iterator end;
    Poco::Logger * log;

    bool nextImpl() override
    {
        if (current == end)
        {
            // Fetch next batch of messages
            bool res = fetchMessages();
            if (!res)
            {
                LOG_ERROR(log, "Consumer error: " << rd_kafka_err2str(rd_kafka_last_error()));
                return false;
            }

            // No error, but no messages read
            if (current == end)
                return false;
        }

        // Process next buffered message
        rd_kafka_message_t * msg = *(current++);
        if (msg->err)
        {
            if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
                LOG_ERROR(log, "Consumer error: " << rd_kafka_err2str(msg->err) << " " << rd_kafka_message_errstr(msg));
            return false;
        }

        // Consume message and mark the topic/partition offset
        BufferBase::set(reinterpret_cast<char *>(msg->payload), msg->len, 0);
        auto err = rd_kafka_offset_store(msg->rkt, msg->partition, msg->offset);
        if (err)
            LOG_ERROR(log, "Failed to store offsets: " << rd_kafka_err2str(err));

        return true;
    }

    void reset()
    {
        for (auto it = messages.begin(); it < end; ++it)
            rd_kafka_message_destroy(*it);

        current = end = messages.begin();
    }

    bool fetchMessages()
    {
        rd_kafka_queue_t* queue = rd_kafka_queue_get_consumer(consumer);
        if (queue == nullptr)
            return false;

        reset();

        auto result = rd_kafka_consume_batch_queue(queue, READ_POLL_MS, messages.data(), messages.size());
        if (result < 0)
            return false;

        current = messages.begin();
        end = current + result;
        return true;
    }

public:
    ReadBufferFromKafkaConsumer(rd_kafka_t * consumer_, size_t batch_size, Poco::Logger * log_)
        : ReadBuffer(nullptr, 0), consumer(consumer_), messages(batch_size), current(messages.begin()), end(messages.begin()), log(log_) {}

    ~ReadBufferFromKafkaConsumer() { reset(); }
};

class KafkaBlockInputStream : public IProfilingBlockInputStream
{
public:

    KafkaBlockInputStream(StorageKafka & storage_, const Context & context_, const String & schema, size_t max_block_size)
        : storage(storage_)
    {
        // Always skip unknown fields regardless of the context (JSON or TSKV)
        Context context = context_;
        context.setSetting("input_format_skip_unknown_fields", UInt64(1));
        if (schema.size() > 0)
            context.setSetting("schema", schema);
        // Create a formatted reader on Kafka messages
        LOG_TRACE(storage.log, "Creating formatted reader");
        read_buf = std::make_unique<ReadBufferFromKafkaConsumer>(storage.consumer, max_block_size, storage.log);
        reader = FormatFactory().getInput(storage.format_name, *read_buf, storage.getSampleBlock(), context, max_block_size);
    }

    ~KafkaBlockInputStream() override
    {
    }

    String getName() const override
    {
        return storage.getName();
    }

    String getID() const override
    {
        std::stringstream res_stream;
        res_stream << "Kafka(" << storage.topics.size() << ", " << storage.format_name << ")";
        return res_stream.str();
    }

    Block readImpl() override
    {
        if (isCancelled())
            return {};

        return reader->read();
    }

    void readPrefixImpl() override
    {
        reader->readPrefix();
    }

    void readSuffixImpl() override
    {
        reader->readSuffix();
    }

private:
    StorageKafka & storage;
    rd_kafka_t * consumer;
    Block sample_block;
    std::unique_ptr<ReadBufferFromKafkaConsumer> read_buf;
    BlockInputStreamPtr reader;
};

StorageKafka::StorageKafka(
    const std::string & table_name_,
    const std::string & database_name_,
    Context & context_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
	const String & brokers_, const String & group_, const Names & topics_,
    const String & format_name_, const String & schema_name_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_},
    table_name(table_name_), database_name(database_name_), context(context_),
    columns(columns_), topics(topics_), format_name(format_name_), schema_name(schema_name_),
    conf(rd_kafka_conf_new()), log(&Logger::get("StorageKafka (" + table_name_ + ")"))
{
    char errstr[512];

    LOG_TRACE(log, "Setting brokers: " << brokers_);
    if (rd_kafka_conf_set(conf, "metadata.broker.list", brokers_.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        throw Exception(String(errstr), ErrorCodes::INCORRECT_DATA);

    LOG_TRACE(log, "Setting Group ID: " << group_ << " Client ID: clickhouse");

    if (rd_kafka_conf_set(conf, "group.id", group_.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        throw Exception(String(errstr), ErrorCodes::INCORRECT_DATA);

    if (rd_kafka_conf_set(conf, "client.id", "clickhouse", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        throw Exception(String(errstr), ErrorCodes::INCORRECT_DATA);

    // Don't store offsets of messages before they're processed
    rd_kafka_conf_set(conf, "enable.auto.offset.store", "false", nullptr, 0);

    // Try to fetch preferred number of bytes before timeout
    const Settings & settings = context.getSettingsRef();
    auto min_bytes = settings.preferred_block_size_bytes.toString();
    rd_kafka_conf_set(conf, "fetch.min.bytes", min_bytes.c_str(), nullptr, 0);
}


BlockInputStreams StorageKafka::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    if (!conf)
    	return BlockInputStreams();

    BlockInputStreams streams;
    streams.reserve(num_streams);

    // Note: The block size is set to 1, otherwise it'd have to be able to return excess buffered messages
    for (size_t i = 0; i < num_streams; ++i)
        streams.push_back(std::make_shared<KafkaBlockInputStream>(*this, context, schema_name, 1));

    LOG_DEBUG(log, "Starting reading " << num_streams << " streams, " << max_block_size << " block size");
    return streams;
}


void StorageKafka::startup()
{
    // Create a consumer from saved configuration
    char errstr[512];
    consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (consumer == nullptr)
        throw Exception("Failed to create consumer handle: " + String(errstr), ErrorCodes::UNKNOWN_EXCEPTION);

    rd_kafka_poll_set_consumer(consumer);

    // Create a list of partitions
    auto * topicList = rd_kafka_topic_partition_list_new(topics.size());
    for (const auto & t : topics)
    {
        LOG_TRACE(log, "Subscribing to topic: " + t);
        rd_kafka_topic_partition_list_add(topicList, t.c_str(), RD_KAFKA_PARTITION_UA);
    }

    // Subscribe to requested topics
    auto err = rd_kafka_subscribe(consumer, topicList);
    if (err)
        throw Exception("Failed to subscribe: " + String(rd_kafka_err2str(err)), ErrorCodes::UNKNOWN_EXCEPTION);

    rd_kafka_topic_partition_list_destroy(topicList);

    // Start the reader thread
    stream_thread = std::thread(&StorageKafka::streamThread, this);
}


void StorageKafka::shutdown()
{
    is_cancelled = true;
    cancel_event.set();

    LOG_TRACE(log, "Unsubscribing from assignments");
    rd_kafka_unsubscribe(consumer);
    auto err = rd_kafka_consumer_close(consumer);
    if (err)
    {
        LOG_ERROR(log, "Failed to close: " + String(rd_kafka_err2str(err)));
    }

    LOG_TRACE(log, "Destroying consumer");
    rd_kafka_destroy(consumer);
    if (stream_thread.joinable())
        stream_thread.join();

    rd_kafka_wait_destroyed(CLEANUP_TIMEOUT_MS);
}


void StorageKafka::updateDependencies()
{
    cancel_event.set();
}


void StorageKafka::streamThread()
{
    setThreadName("KafkaStreamThread");

    while (!is_cancelled)
    {
        try
        {
            auto dependencies = context.getDependencies(database_name, table_name);
            if (dependencies.size() > 0)
            {
                LOG_DEBUG(log, "Started streaming to " << dependencies.size() << " attached views");
                streamToViews();
                LOG_DEBUG(log, "Stopped streaming to views");
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        cancel_event.tryWait(READ_POLL_MS);
    }

    LOG_DEBUG(log, "Stream thread finished");
}


void StorageKafka::streamToViews()
{
    auto table = context.getTable(database_name, table_name);
    if (!table)
    {
        LOG_WARNING(log, "Destination table " << database_name << "." << table_name << " doesn't exist.");
        return;
    }

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->database = database_name;
    insert->table = table_name;
    insert->no_destination = true; // Only insert into dependent views

    // Limit the number of batched messages to allow early cancellations
    const Settings & settings = context.getSettingsRef();
    const size_t block_size = std::min(settings.max_block_size.value, BATCH_SIZE_MAX);
    BlockInputStreamPtr in = std::make_shared<KafkaBlockInputStream>(*this, context, schema_name, block_size);

    // Limit read batch to maximum block size to allow DDL
    IProfilingBlockInputStream::LocalLimits limits;
    limits.max_execution_time = settings.stream_flush_interval_ms;
    limits.timeout_overflow_mode = OverflowMode::BREAK;
    if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(in.get()))
        p_stream->setLimits(limits);

    // Execute the query
    InterpreterInsertQuery interpreter{insert, context};
    auto block_io = interpreter.execute();
    copyData(*in, *block_io.out, &is_cancelled);
}


}

#endif
