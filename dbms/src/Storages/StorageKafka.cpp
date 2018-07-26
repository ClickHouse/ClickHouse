#include <Common/config.h>
#include <Common/config_version.h>
#if USE_RDKAFKA

#include <thread>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Macros.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <Formats/FormatFactory.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageKafka.h> // Y_IGNORE
#include <Storages/StorageFactory.h>
#include <IO/ReadBuffer.h>
#include <common/logger_useful.h>

#if __has_include(<rdkafka.h>) // maybe bundled
#include <rdkafka.h> // Y_IGNORE
#else // system
#include <librdkafka/rdkafka.h> // Y_IGNORE
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_READ_FROM_ISTREAM;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

using namespace Poco::Util;

/// How long to wait for a single message (applies to each individual message)
static const auto READ_POLL_MS       = 500;
static const auto CLEANUP_TIMEOUT_MS = 3000;

/// Configuration prefix
static const String CONFIG_PREFIX = "kafka";

class ReadBufferFromKafkaConsumer : public ReadBuffer
{
    rd_kafka_t * consumer;
    rd_kafka_message_t * current;
    bool current_pending;
    Poco::Logger * log;
    size_t read_messages;
    char row_delimiter;

    bool nextImpl() override
    {
        if (current_pending)
        {
            BufferBase::set(reinterpret_cast<char *>(current->payload), current->len, 0);
            current_pending = false;
            return true;
        }

        // Process next buffered message
        rd_kafka_message_t * msg = rd_kafka_consumer_poll(consumer, READ_POLL_MS);
        if (msg == nullptr)
            return false;

        if (msg->err)
        {
            if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
            {
                LOG_ERROR(log, "Consumer error: " << rd_kafka_err2str(msg->err) << " " << rd_kafka_message_errstr(msg));
                rd_kafka_message_destroy(msg);
                return false;
            }

            // Reach EOF while reading current batch, skip it
            LOG_TRACE(log, "EOF reached for partition " << msg->partition << " offset " << msg->offset);
            rd_kafka_message_destroy(msg);
            return nextImpl();
        }
        ++read_messages;

        // Now we've received a new message. Check if we need to produce a delimiter
        if (row_delimiter != '\0' && current != nullptr)
        {
            BufferBase::set(&row_delimiter, 1, 0);
            reset();
            current = msg;
            current_pending = true;
            return true;
        }

        // Consume message and mark the topic/partition offset
        // The offsets will be committed in the readSuffix() method after the block is completed
        // If an exception is thrown before that would occur, the client will rejoin without committing offsets
        reset();
        current = msg;
        BufferBase::set(reinterpret_cast<char *>(current->payload), current->len, 0);
        return true;
    }

    void reset()
    {
        if (current != nullptr)
        {
            rd_kafka_message_destroy(current);
            current = nullptr;
        }
    }

public:
    ReadBufferFromKafkaConsumer(rd_kafka_t * consumer_, Poco::Logger * log_, char row_delimiter_)
        : ReadBuffer(nullptr, 0), consumer(consumer_), current(nullptr),
        current_pending(false), log(log_), read_messages(0), row_delimiter(row_delimiter_) {
        LOG_TRACE(log, "row delimiter is :" << row_delimiter);
    }

    ~ReadBufferFromKafkaConsumer() { reset(); }

    /// Commit messages read with this consumer
    void commit()
    {
        LOG_TRACE(log, "Committing " << read_messages << " messages");
        if (read_messages == 0)
            return;

        auto err = rd_kafka_commit(consumer, NULL, 1 /* async */);
        if (err)
            throw Exception("Failed to commit offsets: " + String(rd_kafka_err2str(err)), ErrorCodes::UNKNOWN_EXCEPTION);

        read_messages = 0;
    }
};

class KafkaBlockInputStream : public IProfilingBlockInputStream
{
public:

    KafkaBlockInputStream(StorageKafka & storage_, StorageKafka::ConsumerPtr consumer_, const Context & context_, const String & schema, size_t max_block_size)
        : storage(storage_), consumer(consumer_)
    {
        // Always skip unknown fields regardless of the context (JSON or TSKV)
        Context context = context_;
        context.setSetting("input_format_skip_unknown_fields", UInt64(1));
        if (schema.size() > 0)
            context.setSetting("format_schema", schema);

        // Create a formatted reader on Kafka messages
        LOG_TRACE(storage.log, "Creating formatted reader");
        read_buf = std::make_unique<ReadBufferFromKafkaConsumer>(consumer->stream, storage.log, storage.row_delimiter);
        reader = FormatFactory::instance().getInput(storage.format_name, *read_buf, storage.getSampleBlock(), context, max_block_size);
    }

    ~KafkaBlockInputStream() override
    {
        // An error was thrown during the stream or it did not finish successfully
        // The read offsets weren't comitted, so consumer must rejoin the group from the original starting point
        if (!finalized)
        {
            LOG_TRACE(storage.log, "KafkaBlockInputStream did not finish successfully, unsubscribing from assignments and rejoining");
            consumer->unsubscribe();
            consumer->subscribe(storage.topics);
        }

        // Return consumer for another reader
        storage.pushConsumer(consumer);
    }

    String getName() const override
    {
        return storage.getName();
    }

    Block readImpl() override
    {
        if (isCancelledOrThrowIfKilled())
            return {};

        return reader->read();
    }

    Block getHeader() const override { return reader->getHeader(); };

    void readPrefixImpl() override
    {
        // Start reading data
        finalized = false;
        reader->readPrefix();
    }

    void readSuffixImpl() override
    {
        reader->readSuffix();

        // Store offsets read in this stream
        read_buf->commit();

        // Mark as successfully finished
        finalized = true;
    }

private:
    StorageKafka & storage;
    StorageKafka::ConsumerPtr consumer;
    Block sample_block;
    std::unique_ptr<ReadBufferFromKafkaConsumer> read_buf;
    BlockInputStreamPtr reader;
    bool finalized = false;
};

static void loadFromConfig(struct rd_kafka_conf_s * conf, const AbstractConfiguration & config, const std::string & path)
{
    AbstractConfiguration::Keys keys;
    std::vector<char> errstr(512);

    config.keys(path, keys);

    for (const auto & key : keys)
    {
        const String key_path = path + "." + key;
        const String key_name = boost::replace_all_copy(key, "_", ".");
        if (rd_kafka_conf_set(conf, key_name.c_str(), config.getString(key_path).c_str(), errstr.data(), errstr.size()) != RD_KAFKA_CONF_OK)
            throw Exception("Invalid Kafka setting " + key_path + " in config: " + String(errstr.data()), ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
}

StorageKafka::StorageKafka(
    const std::string & table_name_,
    const std::string & database_name_,
    Context & context_,
    const ColumnsDescription & columns_,
    const String & brokers_, const String & group_, const Names & topics_,
    const String & format_name_, char row_delimiter_, const String & schema_name_, size_t num_consumers_)
    : IStorage{columns_},
    table_name(table_name_), database_name(database_name_), context(context_),
    topics(context.getMacros()->expand(topics_)),
    brokers(context.getMacros()->expand(brokers_)),
    group(context.getMacros()->expand(group_)),
    format_name(context.getMacros()->expand(format_name_)),
    row_delimiter(row_delimiter_),
    schema_name(context.getMacros()->expand(schema_name_)),
    num_consumers(num_consumers_), log(&Logger::get("StorageKafka (" + table_name_ + ")")),
    semaphore(0, num_consumers_), mutex(), consumers(), event_update()
{
}


BlockInputStreams StorageKafka::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    if (num_consumers == 0)
        return BlockInputStreams();

    const size_t stream_count = std::min(num_streams, num_consumers);

    BlockInputStreams streams;
    streams.reserve(stream_count);

    // Claim as many consumers as requested, but don't block
    for (size_t i = 0; i < stream_count; ++i)
    {
        auto consumer = tryClaimConsumer(0);
        if (consumer == nullptr)
            break;

        // Use block size of 1, otherwise LIMIT won't work properly as it will buffer excess messages in the last block
        streams.push_back(std::make_shared<KafkaBlockInputStream>(*this, consumer, context, schema_name, 1));
    }

    LOG_DEBUG(log, "Starting reading " << streams.size() << " streams, " << max_block_size << " block size");
    return streams;
}


void StorageKafka::startup()
{
    for (size_t i = 0; i < num_consumers; ++i)
    {
        // Building configuration may throw, the consumer configuration must be destroyed in that case
        auto consumer_conf = rd_kafka_conf_new();
        try
        {
            consumerConfiguration(consumer_conf);
        }
        catch (...)
        {
            rd_kafka_conf_destroy(consumer_conf);
            throw;
        }

        // Create a consumer and subscribe to topics
        // Note: consumer takes ownership of the configuration
        auto consumer = std::make_shared<StorageKafka::Consumer>(consumer_conf);
        consumer->subscribe(topics);

        // Make consumer available
        pushConsumer(consumer);
        ++num_created_consumers;
    }

    // Start the reader thread
    stream_thread = std::thread(&StorageKafka::streamThread, this);
}


void StorageKafka::shutdown()
{
    // Interrupt streaming thread
    stream_cancelled = true;
    event_update.set();

    // Unsubscribe from assignments
    LOG_TRACE(log, "Unsubscribing from assignments");
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto consumer = claimConsumer();
        consumer->unsubscribe();
    }

    // Wait for stream thread to finish
    if (stream_thread.joinable())
        stream_thread.join();

    rd_kafka_wait_destroyed(CLEANUP_TIMEOUT_MS);
}


void StorageKafka::updateDependencies()
{
    event_update.set();
}


void StorageKafka::consumerConfiguration(struct rd_kafka_conf_s * conf)
{
    std::vector<char> errstr(512);

    LOG_TRACE(log, "Setting brokers: " << brokers);
    if (rd_kafka_conf_set(conf, "metadata.broker.list", brokers.c_str(), errstr.data(), errstr.size()) != RD_KAFKA_CONF_OK)
        throw Exception(String(errstr.data()), ErrorCodes::INCORRECT_DATA);

    LOG_TRACE(log, "Setting Group ID: " << group << " Client ID: clickhouse");

    if (rd_kafka_conf_set(conf, "group.id", group.c_str(), errstr.data(), errstr.size()) != RD_KAFKA_CONF_OK)
        throw Exception(String(errstr.data()), ErrorCodes::INCORRECT_DATA);

    if (rd_kafka_conf_set(conf, "client.id", VERSION_FULL, errstr.data(), errstr.size()) != RD_KAFKA_CONF_OK)
        throw Exception(String(errstr.data()), ErrorCodes::INCORRECT_DATA);

    // We manually commit offsets after a stream successfully finished
    rd_kafka_conf_set(conf, "enable.auto.commit", "false", nullptr, 0);

    // Update consumer configuration from the configuration
    const auto & config = context.getConfigRef();
    if (config.has(CONFIG_PREFIX))
        loadFromConfig(conf, config, CONFIG_PREFIX);

    // Update consumer topic-specific configuration
    for (const auto & topic : topics)
    {
        const auto topic_config_key = CONFIG_PREFIX + "_" + topic;
        if (config.has(topic_config_key))
            loadFromConfig(conf, config, topic_config_key);
    }
}

StorageKafka::ConsumerPtr StorageKafka::claimConsumer()
{
    return tryClaimConsumer(-1L);
}

StorageKafka::ConsumerPtr StorageKafka::tryClaimConsumer(long wait_ms)
{
    // Wait for the first free consumer
    if (wait_ms >= 0)
    {
        if (!semaphore.tryWait(wait_ms))
            return nullptr;
    }
    else
        semaphore.wait();

    // Take the first available consumer from the list
    std::lock_guard<std::mutex> lock(mutex);
    auto consumer = consumers.back();
    consumers.pop_back();
    return consumer;
}

void StorageKafka::pushConsumer(StorageKafka::ConsumerPtr c)
{
    std::lock_guard<std::mutex> lock(mutex);
    consumers.push_back(c);
    semaphore.set();
}

void StorageKafka::streamThread()
{
    setThreadName("KafkaStreamThr");

    while (!stream_cancelled)
    {
        try
        {
            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!stream_cancelled)
            {
                // Check if all dependencies are attached
                auto dependencies = context.getDependencies(database_name, table_name);
                if (dependencies.size() == 0)
                    break;
                // Check the dependencies are ready?
                bool ready = true;
                for (const auto & db_tab : dependencies)
                {
                    if (!context.tryGetTable(db_tab.first, db_tab.second))
                        ready = false;
                }
                if (!ready)
                    break;

                LOG_DEBUG(log, "Started streaming to " << dependencies.size() << " attached views");
                streamToViews();
                LOG_DEBUG(log, "Stopped streaming to views");
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        // Wait for attached views
        event_update.tryWait(READ_POLL_MS);
    }

    LOG_DEBUG(log, "Stream thread finished");
}


void StorageKafka::streamToViews()
{
    auto table = context.getTable(database_name, table_name);
    if (!table)
        throw Exception("Engine table " + database_name + "." + table_name + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->database = database_name;
    insert->table = table_name;
    insert->no_destination = true; // Only insert into dependent views

    // Limit the number of batched messages to allow early cancellations
    const Settings & settings = context.getSettingsRef();
    const size_t block_size = settings.max_block_size.value;

    // Create a stream for each consumer and join them in a union stream
    BlockInputStreams streams;
    streams.reserve(num_consumers);
    for (size_t i = 0; i < num_consumers; ++i)
    {
        auto consumer = claimConsumer();
        auto stream = std::make_shared<KafkaBlockInputStream>(*this, consumer, context, schema_name, block_size);
        streams.push_back(stream);

        // Limit read batch to maximum block size to allow DDL
        IProfilingBlockInputStream::LocalLimits limits;
        limits.max_execution_time = settings.stream_flush_interval_ms;
        limits.timeout_overflow_mode = OverflowMode::BREAK;
        if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
            p_stream->setLimits(limits);
    }

    auto in = std::make_shared<UnionBlockInputStream<>>(streams, nullptr, num_consumers);

    // Execute the query
    InterpreterInsertQuery interpreter{insert, context};
    auto block_io = interpreter.execute();
    copyData(*in, *block_io.out, &stream_cancelled);
}


StorageKafka::Consumer::Consumer(struct rd_kafka_conf_s * conf)
{
    std::vector<char> errstr(512);
    stream = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr.data(), errstr.size());
    if (stream == nullptr)
    {
        rd_kafka_conf_destroy(conf);
        throw Exception("Failed to create consumer handle: " + String(errstr.data()), ErrorCodes::UNKNOWN_EXCEPTION);
    }

    rd_kafka_poll_set_consumer(stream);
}


StorageKafka::Consumer::~Consumer()
{
    if (stream != nullptr)
    {
        rd_kafka_consumer_close(stream);
        rd_kafka_destroy(stream);
        stream = nullptr;
    }
}


void StorageKafka::Consumer::subscribe(const Names & topics)
{
    if (stream == nullptr)
        throw Exception("Cannot subscribe to topics when consumer is closed", ErrorCodes::UNKNOWN_EXCEPTION);

    // Create a list of partitions
    auto * topicList = rd_kafka_topic_partition_list_new(topics.size());
    for (const auto & t : topics)
    {
        rd_kafka_topic_partition_list_add(topicList, t.c_str(), RD_KAFKA_PARTITION_UA);
    }

    // Subscribe to requested topics
    auto err = rd_kafka_subscribe(stream, topicList);
    if (err)
    {
        rd_kafka_topic_partition_list_destroy(topicList);
        throw Exception("Failed to subscribe: " + String(rd_kafka_err2str(err)), ErrorCodes::UNKNOWN_EXCEPTION);
    }

    rd_kafka_topic_partition_list_destroy(topicList);
}


void StorageKafka::Consumer::unsubscribe()
{
    if (stream != nullptr)
        rd_kafka_unsubscribe(stream);
}


void registerStorageKafka(StorageFactory & factory)
{
    factory.registerStorage("Kafka", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        /** Arguments of engine is following:
          * - Kafka broker list
          * - List of topics
          * - Group ID (may be a constaint expression with a string result)
          * - Message format (string)
          * - Schema (optional, if the format supports it)
          */

        if (engine_args.size() < 3 || engine_args.size() > 7)
            throw Exception(
                "Storage Kafka requires 3-7 parameters"
                " - Kafka broker list, list of topics to consume, consumer group ID, message format, row delimiter, schema, number of consumers",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String brokers;
        auto ast = typeid_cast<const ASTLiteral *>(engine_args[0].get());
        if (ast && ast->value.getType() == Field::Types::String)
            brokers = safeGet<String>(ast->value);
        else
            throw Exception(String("Kafka broker list must be a string"), ErrorCodes::BAD_ARGUMENTS);

        engine_args[1] = evaluateConstantExpressionAsLiteral(engine_args[1], args.local_context);
        engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);
        engine_args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[3], args.local_context);

        // Parse row delimiter (optional)
        char row_delimiter = '\0';
        if (engine_args.size() >= 5)
        {
            engine_args[4] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[4], args.local_context);

            auto ast = typeid_cast<const ASTLiteral *>(engine_args[4].get());
            String arg;
            if (ast && ast->value.getType() == Field::Types::String)
                arg = safeGet<String>(ast->value);
            else
                throw Exception("Row delimiter must be a char", ErrorCodes::BAD_ARGUMENTS);
            if (arg.size() > 1)
                throw Exception("Row delimiter must be a char", ErrorCodes::BAD_ARGUMENTS);
            else if (arg.size() == 0)
                row_delimiter = '\0';
            else
                row_delimiter = arg[0];
        }

        // Parse format schema if supported (optional)
        String schema;
        if (engine_args.size() >= 6)
        {
            engine_args[5] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[4], args.local_context);

            auto ast = typeid_cast<const ASTLiteral *>(engine_args[5].get());
            if (ast && ast->value.getType() == Field::Types::String)
                schema = safeGet<String>(ast->value);
            else
                throw Exception("Format schema must be a string", ErrorCodes::BAD_ARGUMENTS);
        }

        // Parse number of consumers (optional)
        UInt64 num_consumers = 1;
        if (engine_args.size() >= 7)
        {
            auto ast = typeid_cast<const ASTLiteral *>(engine_args[6].get());
            if (ast && ast->value.getType() == Field::Types::UInt64)
                num_consumers = safeGet<UInt64>(ast->value);
            else
                throw Exception("Number of consumers must be a positive integer", ErrorCodes::BAD_ARGUMENTS);
        }

        // Parse topic list
        Names topics;
        String topic_arg = static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>();
        boost::split(topics, topic_arg , [](char c){ return c == ','; });
        for(String & topic : topics)
            boost::trim(topic);

        // Parse consumer group
        String group = static_cast<const ASTLiteral &>(*engine_args[2]).value.safeGet<String>();

        // Parse format from string
        String format;
        ast = typeid_cast<const ASTLiteral *>(engine_args[3].get());
        if (ast && ast->value.getType() == Field::Types::String)
            format = safeGet<String>(ast->value);
        else
            throw Exception("Format must be a string", ErrorCodes::BAD_ARGUMENTS);

        return StorageKafka::create(
            args.table_name, args.database_name, args.context, args.columns,
            brokers, group, topics, format, row_delimiter, schema, num_consumers);
    });
}


}

#endif
