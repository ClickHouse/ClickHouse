#include <Storages/Kafka/StorageKafka.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/KafkaBlockInputStream.h>
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
}

namespace
{
    const auto RESCHEDULE_MS = 500;
    const auto CLEANUP_TIMEOUT_MS = 3000;

    /// Configuration prefix
    const String CONFIG_PREFIX = "kafka";

    void loadFromConfig(cppkafka::Configuration & conf, const Poco::Util::AbstractConfiguration & config, const std::string & path)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        std::vector<char> errstr(512);

        config.keys(path, keys);

        for (const auto & key : keys)
        {
            const String key_path = path + "." + key;
            const String key_name = boost::replace_all_copy(key, "_", ".");
            conf.set(key_name, config.getString(key_path));
        }
    }
} // namespace

StorageKafka::StorageKafka(
    const std::string & table_name_,
    const std::string & database_name_,
    Context & context_,
    const ColumnsDescription & columns_,
    const String & brokers_, const String & group_, const Names & topics_,
    const String & format_name_, char row_delimiter_, const String & schema_name_,
    size_t num_consumers_, UInt64 max_block_size_, size_t skip_broken_)
    : IStorage{columns_},
    table_name(table_name_), database_name(database_name_), global_context(context_),
    topics(global_context.getMacros()->expand(topics_)),
    brokers(global_context.getMacros()->expand(brokers_)),
    group(global_context.getMacros()->expand(group_)),
    format_name(global_context.getMacros()->expand(format_name_)),
    row_delimiter(row_delimiter_),
    schema_name(global_context.getMacros()->expand(schema_name_)),
    num_consumers(num_consumers_), max_block_size(max_block_size_), log(&Logger::get("StorageKafka (" + table_name_ + ")")),
    semaphore(0, num_consumers_),
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
        // Make buffer available
        pushBuffer(createBuffer());
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
        auto buffer = claimBuffer();
        // FIXME: not sure if we really close consumers here, and if we really need to close them here.
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

    // If no offset stored for this group, read all messages from the start
    conf.set("auto.offset.reset", "smallest");

    // We manually commit offsets after a stream successfully finished
    conf.set("enable.auto.commit", "false");

    // Ignore EOF messages
    conf.set("enable.partition.eof", "false");

    // for debug logs inside rdkafka
    // conf.set("debug", "consumer,cgrp,topic,fetch");

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

BufferPtr StorageKafka::createBuffer()
{
    // Create a consumer and subscribe to topics
    auto consumer = std::make_shared<cppkafka::Consumer>(createConsumerConfiguration());
    consumer->subscribe(topics);

    // Limit the number of batched messages to allow early cancellations
    const Settings & settings = global_context.getSettingsRef();
    size_t batch_size = max_block_size;
    if (!batch_size)
        batch_size = settings.max_block_size.value;

    return std::make_shared<DelimitedReadBuffer>(new ReadBufferFromKafkaConsumer(consumer, log, batch_size), row_delimiter);
}

BufferPtr StorageKafka::claimBuffer()
{
    return tryClaimBuffer(-1L);
}

BufferPtr StorageKafka::tryClaimBuffer(long wait_ms)
{
    // Wait for the first free buffer
    if (wait_ms >= 0)
    {
        if (!semaphore.tryWait(wait_ms))
            return nullptr;
    }
    else
        semaphore.wait();

    // Take the first available buffer from the list
    std::lock_guard lock(mutex);
    auto buffer = buffers.back();
    buffers.pop_back();
    return buffer;
}

void StorageKafka::pushBuffer(BufferPtr buffer)
{
    std::lock_guard lock(mutex);
    buffers.push_back(buffer);
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
        task->scheduleAfter(RESCHEDULE_MS);
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
            const auto * ast = engine_args[0]->as<ASTLiteral>();
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
            topic_list = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
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
            group = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
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

            const auto * ast = engine_args[3]->as<ASTLiteral>();
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

            const auto * ast = engine_args[4]->as<ASTLiteral>();
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

            const auto * ast = engine_args[5]->as<ASTLiteral>();
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
            const auto * ast = engine_args[6]->as<ASTLiteral>();
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
        UInt64 max_block_size = 0;
        if (args_count >= 8)
        {
            const auto * ast = engine_args[7]->as<ASTLiteral>();
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
            const auto * ast = engine_args[8]->as<ASTLiteral>();
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
