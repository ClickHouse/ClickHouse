#include <Storages/Kafka/StorageKafka.h>

#if USE_RDKAFKA

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/config_version.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>


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
    extern const int TIMEOUT_EXCEEDED;
}

using namespace Poco::Util;

/// How long to wait for a single message (applies to each individual message)
static const auto READ_POLL_MS       = 500;
static const auto CLEANUP_TIMEOUT_MS = 3000;

/// Configuration prefix
static const String CONFIG_PREFIX = "kafka";

class ReadBufferFromKafkaConsumer : public ReadBuffer
{
    ConsumerPtr consumer;
    cppkafka::Message current;
    bool current_pending = false;   /// We've fetched "current" message and need to process it on the next iteration.
    Poco::Logger * log;
    size_t read_messages = 0;
    char row_delimiter;

    bool nextImpl() override
    {
        if (current_pending)
        {
            // XXX: very fishy place with const casting.
            BufferBase::set(reinterpret_cast<char *>(const_cast<unsigned char *>(current.get_payload().get_data())), current.get_payload().get_size(), 0);
            current_pending = false;
            return true;
        }

        // Process next buffered message
        auto message = consumer->poll(std::chrono::milliseconds(READ_POLL_MS));
        if (!message)
            return false;

        if (message.is_eof())
        {
            // Reached EOF while reading current batch, skip it.
            LOG_TRACE(log, "EOF reached for partition " << message.get_partition() << " offset " << message.get_offset());
            return nextImpl();
        }
        else if (auto err = message.get_error())
        {
            LOG_ERROR(log, "Consumer error: " << err);
            return false;
        }

        ++read_messages;

        // Now we've received a new message. Check if we need to produce a delimiter
        if (row_delimiter != '\0' && current)
        {
            BufferBase::set(&row_delimiter, 1, 0);
            current = std::move(message);
            current_pending = true;
            return true;
        }

        // Consume message and mark the topic/partition offset
        // The offsets will be committed in the readSuffix() method after the block is completed
        // If an exception is thrown before that would occur, the client will rejoin without committing offsets
        current = std::move(message);

        // XXX: very fishy place with const casting.
        BufferBase::set(reinterpret_cast<char *>(const_cast<unsigned char *>(current.get_payload().get_data())), current.get_payload().get_size(), 0);
        return true;
    }

public:
    ReadBufferFromKafkaConsumer(ConsumerPtr consumer_, Poco::Logger * log_, char row_delimiter_)
        : ReadBuffer(nullptr, 0), consumer(consumer_), log(log_), row_delimiter(row_delimiter_)
    {
        if (row_delimiter != '\0')
            LOG_TRACE(log, "Row delimiter is: " << row_delimiter);
    }

    /// Commit messages read with this consumer
    void commit()
    {
        LOG_TRACE(log, "Committing " << read_messages << " messages");
        if (read_messages == 0)
            return;

        consumer->async_commit();
        read_messages = 0;
    }
};

class KafkaBlockInputStream : public IBlockInputStream
{
public:
    KafkaBlockInputStream(StorageKafka & storage_, const Context & context_, const String & schema, size_t max_block_size_)
        : storage(storage_), context(context_), max_block_size(max_block_size_)
    {
        // Always skip unknown fields regardless of the context (JSON or TSKV)
        context.setSetting("input_format_skip_unknown_fields", 1u);

        // We don't use ratio since the number of Kafka messages may vary from stream to stream.
        // Thus, ratio is meaningless.
        context.setSetting("input_format_allow_errors_ratio", 1.);
        context.setSetting("input_format_allow_errors_num", storage.skip_broken);

        if (schema.size() > 0)
            context.setSetting("format_schema", schema);
    }

    ~KafkaBlockInputStream() override
    {
        if (!hasClaimed())
            return;

        // An error was thrown during the stream or it did not finish successfully
        // The read offsets weren't committed, so consumer must rejoin the group from the original starting point
        if (!finalized)
        {
            LOG_TRACE(storage.log, "KafkaBlockInputStream did not finish successfully, unsubscribing from assignments and rejoining");
            consumer->unsubscribe();
            consumer->subscribe(storage.topics);
        }

        // Return consumer for another reader
        storage.pushConsumer(consumer);
        consumer = nullptr;
    }

    String getName() const override
    {
        return storage.getName();
    }

    Block readImpl() override
    {
        if (isCancelledOrThrowIfKilled() || !hasClaimed())
            return {};

        if (!reader)
            throw Exception("Logical error: reader is not initialized", ErrorCodes::LOGICAL_ERROR);

        return reader->read();
    }

    Block getHeader() const override { return storage.getSampleBlock(); }

    void readPrefixImpl() override
    {
        if (!hasClaimed())
        {
            // Create a formatted reader on Kafka messages
            LOG_TRACE(storage.log, "Creating formatted reader");
            consumer = storage.tryClaimConsumer(context.getSettingsRef().queue_max_wait_ms.totalMilliseconds());
            if (consumer == nullptr)
                throw Exception("Failed to claim consumer: ", ErrorCodes::TIMEOUT_EXCEEDED);

            read_buf = std::make_unique<ReadBufferFromKafkaConsumer>(consumer, storage.log, storage.row_delimiter);
            reader = FormatFactory::instance().getInput(storage.format_name, *read_buf, storage.getSampleBlock(), context, max_block_size);
        }

        // Start reading data
        finalized = false;
        reader->readPrefix();
    }

    void readSuffixImpl() override
    {
        if (hasClaimed())
        {
            reader->readSuffix();
            // Store offsets read in this stream
            read_buf->commit();
        }

        // Mark as successfully finished
        finalized = true;
    }

private:
    StorageKafka & storage;
    ConsumerPtr consumer;
    Context context;
    size_t max_block_size;
    Block sample_block;
    std::unique_ptr<ReadBufferFromKafkaConsumer> read_buf;
    BlockInputStreamPtr reader;
    bool finalized = false;

    // Return true if consumer has been claimed by the stream
    bool hasClaimed() { return consumer != nullptr; }
};

static void loadFromConfig(cppkafka::Configuration & conf, const AbstractConfiguration & config, const std::string & path)
{
    AbstractConfiguration::Keys keys;
    std::vector<char> errstr(512);

    config.keys(path, keys);

    for (const auto & key : keys)
    {
        const String key_path = path + "." + key;
        const String key_name = boost::replace_all_copy(key, "_", ".");
        conf.set(key_name, config.getString(key_path));
    }
}

StorageKafka::StorageKafka(
    const std::string & table_name_,
    const std::string & database_name_,
    Context & context_,
    const ColumnsDescription & columns_,
    const String & brokers_, const String & group_, const Names & topics_,
    const String & format_name_, char row_delimiter_, const String & schema_name_,
    size_t num_consumers_, size_t max_block_size_, size_t skip_broken_)
    : IStorage{columns_},
    table_name(table_name_), database_name(database_name_), global_context(context_),
    topics(global_context.getMacros()->expand(topics_)),
    brokers(global_context.getMacros()->expand(brokers_)),
    group(global_context.getMacros()->expand(group_)),
    format_name(global_context.getMacros()->expand(format_name_)),
    row_delimiter(row_delimiter_),
    schema_name(global_context.getMacros()->expand(schema_name_)),
    num_consumers(num_consumers_), max_block_size(max_block_size_), log(&Logger::get("StorageKafka (" + table_name_ + ")")),
    semaphore(0, num_consumers_), mutex(), consumers(),
    skip_broken(skip_broken_)
{
    task = global_context.getSchedulePool().createTask(log->name(), [this]{ streamThread(); });
    task->deactivate();
}


BlockInputStreams StorageKafka::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    check(column_names);

    if (num_created_consumers == 0)
        return BlockInputStreams();

    const size_t stream_count = std::min(size_t(num_streams), num_created_consumers);

    BlockInputStreams streams;
    streams.reserve(stream_count);

    // Claim as many consumers as requested, but don't block
    for (size_t i = 0; i < stream_count; ++i)
    {
        // Use block size of 1, otherwise LIMIT won't work properly as it will buffer excess messages in the last block
        // TODO That leads to awful performance.
        streams.emplace_back(std::make_shared<KafkaBlockInputStream>(*this, context, schema_name, 1));
    }

    LOG_DEBUG(log, "Starting reading " << streams.size() << " streams");
    return streams;
}


void StorageKafka::startup()
{
    for (size_t i = 0; i < num_consumers; ++i)
    {
        // Create a consumer and subscribe to topics
        auto consumer = std::make_shared<cppkafka::Consumer>(createConsumerConfiguration());
        consumer->subscribe(topics);

        // Make consumer available
        pushConsumer(consumer);
        ++num_created_consumers;
    }

    // Start the reader thread
    task->activateAndSchedule();
}


void StorageKafka::shutdown()
{
    // Interrupt streaming thread
    stream_cancelled = true;

    // Close all consumers
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto consumer = claimConsumer();
        // FIXME: not sure if really close consumers here, and if we really need to close them here.
    }

    LOG_TRACE(log, "Waiting for cleanup");
    rd_kafka_wait_destroyed(CLEANUP_TIMEOUT_MS);

    task->deactivate();
}


void StorageKafka::updateDependencies()
{
    task->activateAndSchedule();
}


cppkafka::Configuration StorageKafka::createConsumerConfiguration()
{
    cppkafka::Configuration conf;

    LOG_TRACE(log, "Setting brokers: " << brokers);
    conf.set("metadata.broker.list", brokers);

    LOG_TRACE(log, "Setting Group ID: " << group << " Client ID: clickhouse");
    conf.set("group.id", group);

    conf.set("client.id", VERSION_FULL);

    // We manually commit offsets after a stream successfully finished
    conf.set("enable.auto.commit", "false");

    // Update consumer configuration from the configuration
    const auto & config = global_context.getConfigRef();
    if (config.has(CONFIG_PREFIX))
        loadFromConfig(conf, config, CONFIG_PREFIX);

    // Update consumer topic-specific configuration
    for (const auto & topic : topics)
    {
        const auto topic_config_key = CONFIG_PREFIX + "_" + topic;
        if (config.has(topic_config_key))
            loadFromConfig(conf, config, topic_config_key);
    }

    return conf;
}

ConsumerPtr StorageKafka::claimConsumer()
{
    return tryClaimConsumer(-1L);
}

ConsumerPtr StorageKafka::tryClaimConsumer(long wait_ms)
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
    std::lock_guard lock(mutex);
    auto consumer = consumers.back();
    consumers.pop_back();
    return consumer;
}

void StorageKafka::pushConsumer(ConsumerPtr consumer)
{
    std::lock_guard lock(mutex);
    consumers.push_back(consumer);
    semaphore.set();
}

bool StorageKafka::checkDependencies(const String & current_database_name, const String & current_table_name)
{
    // Check if all dependencies are attached
    auto dependencies = global_context.getDependencies(current_database_name, current_table_name);
    if (dependencies.size() == 0)
        return true;

    // Check the dependencies are ready?
    for (const auto & db_tab : dependencies)
    {
        auto table = global_context.tryGetTable(db_tab.first, db_tab.second);
        if (!table)
            return false;

        // If it materialized view, check it's target table
        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(table.get());
        if (materialized_view && !materialized_view->tryGetTargetTable())
            return false;

        // Check all its dependencies
        if (!checkDependencies(db_tab.first, db_tab.second))
            return false;
    }

    return true;
}

void StorageKafka::streamThread()
{
    try
    {
        // Check if at least one direct dependency is attached
        auto dependencies = global_context.getDependencies(database_name, table_name);

        // Keep streaming as long as there are attached views and streaming is not cancelled
        while (!stream_cancelled && num_created_consumers > 0 && dependencies.size() > 0)
        {
            if (!checkDependencies(database_name, table_name))
                break;

            LOG_DEBUG(log, "Started streaming to " << dependencies.size() << " attached views");

            // Reschedule if not limited
            if (!streamToViews())
                break;
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    // Wait for attached views
    if (!stream_cancelled)
        task->scheduleAfter(READ_POLL_MS);
}


bool StorageKafka::streamToViews()
{
    auto table = global_context.getTable(database_name, table_name);
    if (!table)
        throw Exception("Engine table " + database_name + "." + table_name + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->database = database_name;
    insert->table = table_name;
    insert->no_destination = true; // Only insert into dependent views

    // Limit the number of batched messages to allow early cancellations
    const Settings & settings = global_context.getSettingsRef();
    size_t block_size = max_block_size;
    if (block_size == 0)
        block_size = settings.max_block_size.value;

    // Create a stream for each consumer and join them in a union stream
    BlockInputStreams streams;
    streams.reserve(num_created_consumers);
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto stream = std::make_shared<KafkaBlockInputStream>(*this, global_context, schema_name, block_size);
        streams.emplace_back(stream);

        // Limit read batch to maximum block size to allow DDL
        IBlockInputStream::LocalLimits limits;
        limits.max_execution_time = settings.stream_flush_interval_ms;
        limits.timeout_overflow_mode = OverflowMode::BREAK;
        stream->setLimits(limits);
    }

    // Join multiple streams if necessary
    BlockInputStreamPtr in;
    if (streams.size() > 1)
        in = std::make_shared<UnionBlockInputStream>(streams, nullptr, streams.size());
    else
        in = streams[0];

    // Execute the query
    InterpreterInsertQuery interpreter{insert, global_context};
    auto block_io = interpreter.execute();
    copyData(*in, *block_io.out, &stream_cancelled);

    // Check whether the limits were applied during query execution
    bool limits_applied = false;
    const BlockStreamProfileInfo & info = in->getProfileInfo();
    limits_applied = info.hasAppliedLimit();

    return limits_applied;
}


void registerStorageKafka(StorageFactory & factory)
{
    factory.registerStorage("Kafka", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        size_t args_count = engine_args.size();
        bool has_settings = args.storage_def->settings;

        KafkaSettings kafka_settings;
        if (has_settings)
        {
            kafka_settings.loadFromQuery(*args.storage_def);
        }

        /** Arguments of engine is following:
          * - Kafka broker list
          * - List of topics
          * - Group ID (may be a constaint expression with a string result)
          * - Message format (string)
          * - Row delimiter
          * - Schema (optional, if the format supports it)
          * - Number of consumers
          * - Max block size for background consumption
          * - Skip (at least) unreadable messages number
          */

        // Check arguments and settings
        #define CHECK_KAFKA_STORAGE_ARGUMENT(ARG_NUM, PAR_NAME)            \
            /* One of the four required arguments is not specified */      \
            if (args_count < ARG_NUM && ARG_NUM <= 4 &&                    \
                !kafka_settings.PAR_NAME.changed)                          \
            {                                                              \
                throw Exception(                                           \
                    "Required parameter '" #PAR_NAME "' "                  \
                    "for storage Kafka not specified",                     \
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);         \
            }                                                              \
            /* The same argument is given in two places */                 \
            if (has_settings &&                                            \
                kafka_settings.PAR_NAME.changed &&                         \
                args_count >= ARG_NUM)                                     \
            {                                                              \
                throw Exception(                                           \
                    "The argument №" #ARG_NUM " of storage Kafka "         \
                    "and the parameter '" #PAR_NAME "' "                   \
                    "in SETTINGS cannot be specified at the same time",    \
                    ErrorCodes::BAD_ARGUMENTS);                            \
            }

        CHECK_KAFKA_STORAGE_ARGUMENT(1, kafka_broker_list)
        CHECK_KAFKA_STORAGE_ARGUMENT(2, kafka_topic_list)
        CHECK_KAFKA_STORAGE_ARGUMENT(3, kafka_group_name)
        CHECK_KAFKA_STORAGE_ARGUMENT(4, kafka_format)
        CHECK_KAFKA_STORAGE_ARGUMENT(5, kafka_row_delimiter)
        CHECK_KAFKA_STORAGE_ARGUMENT(6, kafka_schema)
        CHECK_KAFKA_STORAGE_ARGUMENT(7, kafka_num_consumers)
        CHECK_KAFKA_STORAGE_ARGUMENT(8, kafka_max_block_size)
        CHECK_KAFKA_STORAGE_ARGUMENT(9, kafka_skip_broken_messages)
        #undef CHECK_KAFKA_STORAGE_ARGUMENT

        // Get and check broker list
        String brokers;
        if (args_count >= 1)
        {
            auto ast = typeid_cast<const ASTLiteral *>(engine_args[0].get());
            if (ast && ast->value.getType() == Field::Types::String)
            {
                brokers = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception(String("Kafka broker list must be a string"), ErrorCodes::BAD_ARGUMENTS);
            }
        }
        else if (kafka_settings.kafka_broker_list.changed)
        {
            brokers = kafka_settings.kafka_broker_list.value;
        }

        // Get and check topic list
        String topic_list;
        if (args_count >= 2)
        {
            engine_args[1] = evaluateConstantExpressionAsLiteral(engine_args[1], args.local_context);
            topic_list = static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>();
        }
        else if (kafka_settings.kafka_topic_list.changed)
        {
            topic_list = kafka_settings.kafka_topic_list.value;
        }
        Names topics;
        boost::split(topics, topic_list , [](char c){ return c == ','; });
        for (String & topic : topics)
        {
            boost::trim(topic);
        }

        // Get and check group name
        String group;
        if (args_count >= 3)
        {
            engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);
            group = static_cast<const ASTLiteral &>(*engine_args[2]).value.safeGet<String>();
        }
        else if (kafka_settings.kafka_group_name.changed)
        {
            group = kafka_settings.kafka_group_name.value;
        }

        // Get and check message format name
        String format;
        if (args_count >= 4)
        {
            engine_args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[3], args.local_context);

            auto ast = typeid_cast<const ASTLiteral *>(engine_args[3].get());
            if (ast && ast->value.getType() == Field::Types::String)
            {
                format = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception("Format must be a string", ErrorCodes::BAD_ARGUMENTS);
            }
        }
        else if (kafka_settings.kafka_format.changed)
        {
            format = kafka_settings.kafka_format.value;
        }

        // Parse row delimiter (optional)
        char row_delimiter = '\0';
        if (args_count >= 5)
        {
            engine_args[4] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[4], args.local_context);

            auto ast = typeid_cast<const ASTLiteral *>(engine_args[4].get());
            String arg;
            if (ast && ast->value.getType() == Field::Types::String)
            {
                arg = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception("Row delimiter must be a char", ErrorCodes::BAD_ARGUMENTS);
            }
            if (arg.size() > 1)
            {
                throw Exception("Row delimiter must be a char", ErrorCodes::BAD_ARGUMENTS);
            }
            else if (arg.size() == 0)
            {
                row_delimiter = '\0';
            }
            else
            {
                row_delimiter = arg[0];
            }
        }
        else if (kafka_settings.kafka_row_delimiter.changed)
        {
            row_delimiter = kafka_settings.kafka_row_delimiter.value;
        }

        // Parse format schema if supported (optional)
        String schema;
        if (args_count >= 6)
        {
            engine_args[5] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[5], args.local_context);

            auto ast = typeid_cast<const ASTLiteral *>(engine_args[5].get());
            if (ast && ast->value.getType() == Field::Types::String)
            {
                schema = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception("Format schema must be a string", ErrorCodes::BAD_ARGUMENTS);
            }
        }
        else if (kafka_settings.kafka_schema.changed)
        {
            schema = kafka_settings.kafka_schema.value;
        }

        // Parse number of consumers (optional)
        UInt64 num_consumers = 1;
        if (args_count >= 7)
        {
            auto ast = typeid_cast<const ASTLiteral *>(engine_args[6].get());
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                num_consumers = safeGet<UInt64>(ast->value);
            }
            else
            {
                throw Exception("Number of consumers must be a positive integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }
        else if (kafka_settings.kafka_num_consumers.changed)
        {
            num_consumers = kafka_settings.kafka_num_consumers.value;
        }

        // Parse max block size (optional)
        size_t max_block_size = 0;
        if (args_count >= 8)
        {
            auto ast = typeid_cast<const ASTLiteral *>(engine_args[7].get());
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                max_block_size = static_cast<size_t>(safeGet<UInt64>(ast->value));
            }
            else
            {
                // TODO: no check if the integer is really positive
                throw Exception("Maximum block size must be a positive integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }
        else if (kafka_settings.kafka_max_block_size.changed)
        {
            max_block_size = static_cast<size_t>(kafka_settings.kafka_max_block_size.value);
        }

        size_t skip_broken = 0;
        if (args_count >= 9)
        {
            auto ast = typeid_cast<const ASTLiteral *>(engine_args[8].get());
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                skip_broken = static_cast<size_t>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Number of broken messages to skip must be a non-negative integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }
        else if (kafka_settings.kafka_skip_broken_messages.changed)
        {
            skip_broken = static_cast<size_t>(kafka_settings.kafka_skip_broken_messages.value);
        }

        return StorageKafka::create(
            args.table_name, args.database_name, args.context, args.columns,
            brokers, group, topics, format, row_delimiter, schema, num_consumers, max_block_size, skip_broken);
    });
}


}

#endif
