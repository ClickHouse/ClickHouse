#include <Storages/Kafka/StorageKafka.h>
#include <Storages/Kafka/parseSyslogLevel.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/KafkaBlockInputStream.h>
#include <Storages/Kafka/KafkaBlockOutputStream.h>
#include <Storages/Kafka/WriteBufferToKafkaProducer.h>
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
#include <Common/quoteString.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <librdkafka/rdkafka.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    const auto RESCHEDULE_MS = 500;
    const auto CLEANUP_TIMEOUT_MS = 3000;
    const auto MAX_THREAD_WORK_DURATION_MS = 60000;  // once per minute leave do reschedule (we can't lock threads in pool forever)

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

    rd_kafka_resp_err_t rdKafkaOnThreadStart(rd_kafka_t *, rd_kafka_thread_type_t thread_type, const char *, void * ctx)
    {
        StorageKafka * self = reinterpret_cast<StorageKafka *>(ctx);

        const auto & storage_id = self->getStorageID();
        const auto & table = storage_id.getTableName();

        switch (thread_type)
        {
            case RD_KAFKA_THREAD_MAIN:
                setThreadName(("rdk:m/" + table.substr(0, 9)).c_str());
                break;
            case RD_KAFKA_THREAD_BACKGROUND:
                setThreadName(("rdk:bg/" + table.substr(0, 8)).c_str());
                break;
            case RD_KAFKA_THREAD_BROKER:
                setThreadName(("rdk:b/" + table.substr(0, 9)).c_str());
                break;
        }
        return RD_KAFKA_RESP_ERR_NO_ERROR;
    }

    rd_kafka_resp_err_t rdKafkaOnNew(rd_kafka_t * rk, const rd_kafka_conf_t *, void * ctx, char * /*errstr*/, size_t /*errstr_size*/)
    {
        return rd_kafka_interceptor_add_on_thread_start(rk, "setThreadName", rdKafkaOnThreadStart, ctx);
    }

    rd_kafka_resp_err_t rdKafkaOnConfDup(rd_kafka_conf_t * new_conf, const rd_kafka_conf_t * /*old_conf*/, size_t /*filter_cnt*/, const char ** /*filter*/, void * ctx)
    {
        rd_kafka_resp_err_t status;

        // cppkafka copies configuration multiple times
        status = rd_kafka_conf_interceptor_add_on_conf_dup(new_conf, "setThreadName", rdKafkaOnConfDup, ctx);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            return status;

        status = rd_kafka_conf_interceptor_add_on_new(new_conf, "setThreadName", rdKafkaOnNew, ctx);
        return status;
    }
}

StorageKafka::StorageKafka(
    const StorageID & table_id_,
    Context & context_,
    const ColumnsDescription & columns_,
    const String & brokers_,
    const String & group_,
    const Names & topics_,
    const String & format_name_,
    char row_delimiter_,
    const String & schema_name_,
    size_t num_consumers_,
    UInt64 max_block_size_,
    size_t skip_broken_,
    bool intermediate_commit_)
    : IStorage(table_id_)
    , global_context(context_.getGlobalContext())
    , kafka_context(Context(global_context))
    , topics(global_context.getMacros()->expand(topics_))
    , brokers(global_context.getMacros()->expand(brokers_))
    , group(global_context.getMacros()->expand(group_))
    , format_name(global_context.getMacros()->expand(format_name_))
    , row_delimiter(row_delimiter_)
    , schema_name(global_context.getMacros()->expand(schema_name_))
    , num_consumers(num_consumers_)
    , max_block_size(max_block_size_)
    , log(&Logger::get("StorageKafka (" + table_id_.table_name + ")"))
    , semaphore(0, num_consumers_)
    , skip_broken(skip_broken_)
    , intermediate_commit(intermediate_commit_)
{
    kafka_context.makeQueryContext();

    setColumns(columns_);
    task = global_context.getSchedulePool().createTask(log->name(), [this]{ threadFunc(); });
    task->deactivate();
}


Pipes StorageKafka::read(
    const Names & column_names,
    const SelectQueryInfo & /* query_info */,
    const Context & context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    unsigned /* num_streams */)
{
    if (num_created_consumers == 0)
        return {};

    /// Always use all consumers at once, otherwise SELECT may not read messages from all partitions.
    Pipes pipes;
    pipes.reserve(num_created_consumers);

    // Claim as many consumers as requested, but don't block
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        /// Use block size of 1, otherwise LIMIT won't work properly as it will buffer excess messages in the last block
        /// TODO: probably that leads to awful performance.
        /// FIXME: seems that doesn't help with extra reading and committing unprocessed messages.
        /// TODO: rewrite KafkaBlockInputStream to KafkaSource. Now it is used in other place.
        pipes.emplace_back(std::make_shared<SourceFromInputStream>(std::make_shared<KafkaBlockInputStream>(*this, context, column_names, 1)));
    }

    LOG_DEBUG(log, "Starting reading {} streams", pipes.size());
    return pipes;
}


BlockOutputStreamPtr StorageKafka::write(const ASTPtr &, const Context & context)
{
    if (topics.size() > 1)
        throw Exception("Can't write to Kafka table with multiple topics!", ErrorCodes::NOT_IMPLEMENTED);
    return std::make_shared<KafkaBlockOutputStream>(*this, context);
}


void StorageKafka::startup()
{
    for (size_t i = 0; i < num_consumers; ++i)
    {
        try
        {
            pushReadBuffer(createReadBuffer());
            ++num_created_consumers;
        }
        catch (const cppkafka::Exception &)
        {
            tryLogCurrentException(log);
        }
    }

    // Start the reader thread
    task->activateAndSchedule();
}


void StorageKafka::shutdown()
{
    // Interrupt streaming thread
    stream_cancelled = true;

    LOG_TRACE(log, "Waiting for cleanup");
    task->deactivate();

    // Close all consumers
    for (size_t i = 0; i < num_created_consumers; ++i)
        auto buffer = popReadBuffer();

    rd_kafka_wait_destroyed(CLEANUP_TIMEOUT_MS);
}


void StorageKafka::pushReadBuffer(ConsumerBufferPtr buffer)
{
    std::lock_guard lock(mutex);
    buffers.push_back(buffer);
    semaphore.set();
}


ConsumerBufferPtr StorageKafka::popReadBuffer()
{
    return popReadBuffer(std::chrono::milliseconds::zero());
}


ConsumerBufferPtr StorageKafka::popReadBuffer(std::chrono::milliseconds timeout)
{
    // Wait for the first free buffer
    if (timeout == std::chrono::milliseconds::zero())
        semaphore.wait();
    else
    {
        if (!semaphore.tryWait(timeout.count()))
            return nullptr;
    }

    // Take the first available buffer from the list
    std::lock_guard lock(mutex);
    auto buffer = buffers.back();
    buffers.pop_back();
    return buffer;
}


ProducerBufferPtr StorageKafka::createWriteBuffer(const Block & header)
{
    cppkafka::Configuration conf;
    conf.set("metadata.broker.list", brokers);
    conf.set("group.id", group);
    conf.set("client.id", VERSION_FULL);
    // TODO: fill required settings
    updateConfiguration(conf);

    auto producer = std::make_shared<cppkafka::Producer>(conf);
    const Settings & settings = global_context.getSettingsRef();
    size_t poll_timeout = settings.stream_poll_timeout_ms.totalMilliseconds();

    return std::make_shared<WriteBufferToKafkaProducer>(
        producer, topics[0], row_delimiter ? std::optional<char>{row_delimiter} : std::nullopt, 1, 1024, std::chrono::milliseconds(poll_timeout), header);
}


ConsumerBufferPtr StorageKafka::createReadBuffer()
{
    cppkafka::Configuration conf;

    conf.set("metadata.broker.list", brokers);
    conf.set("group.id", group);
    conf.set("client.id", VERSION_FULL);

    conf.set("auto.offset.reset", "smallest");     // If no offset stored for this group, read all messages from the start

    updateConfiguration(conf);

    // those settings should not be changed by users.
    conf.set("enable.auto.commit", "false");       // We manually commit offsets after a stream successfully finished
    conf.set("enable.auto.offset.store", "false"); // Update offset automatically - to commit them all at once.
    conf.set("enable.partition.eof", "false");     // Ignore EOF messages

    // Create a consumer and subscribe to topics
    auto consumer = std::make_shared<cppkafka::Consumer>(conf);
    consumer->set_destroy_flags(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

    // Limit the number of batched messages to allow early cancellations
    const Settings & settings = global_context.getSettingsRef();
    size_t batch_size = max_block_size;
    if (!batch_size)
        batch_size = settings.max_block_size.value;
    size_t poll_timeout = settings.stream_poll_timeout_ms.totalMilliseconds();

    /// NOTE: we pass |stream_cancelled| by reference here, so the buffers should not outlive the storage.
    return std::make_shared<ReadBufferFromKafkaConsumer>(consumer, log, batch_size, poll_timeout, intermediate_commit, stream_cancelled, getTopics());
}


void StorageKafka::updateConfiguration(cppkafka::Configuration & conf)
{
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

    // No need to add any prefix, messages can be distinguished
    conf.set_log_callback([this](cppkafka::KafkaHandleBase &, int level, const std::string & /* facility */, const std::string & message)
    {
        auto [poco_level, client_logs_level] = parseSyslogLevel(level);
        LOG_IMPL(log, client_logs_level, poco_level, message);
    });

    // Configure interceptor to change thread name
    //
    // TODO: add interceptors support into the cppkafka.
    // XXX:  rdkafka uses pthread_set_name_np(), but glibc-compatibliity overrides it to noop.
    {
        // This should be safe, since we wait the rdkafka object anyway.
        void * self = static_cast<void *>(this);

        int status;

        status = rd_kafka_conf_interceptor_add_on_new(conf.get_handle(), "setThreadName", rdKafkaOnNew, self);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(log, "Cannot set new interceptor");

        // cppkafka always copy the configuration
        status = rd_kafka_conf_interceptor_add_on_conf_dup(conf.get_handle(), "setThreadName", rdKafkaOnConfDup, self);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(log, "Cannot set dup conf interceptor");
    }
}

bool StorageKafka::checkDependencies(const StorageID & table_id)
{
    // Check if all dependencies are attached
    auto dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    if (dependencies.empty())
        return true;

    // Check the dependencies are ready?
    for (const auto & db_tab : dependencies)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(db_tab);
        if (!table)
            return false;

        // If it materialized view, check it's target table
        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(table.get());
        if (materialized_view && !materialized_view->tryGetTargetTable())
            return false;

        // Check all its dependencies
        if (!checkDependencies(db_tab))
            return false;
    }

    return true;
}

void StorageKafka::threadFunc()
{
    try
    {
        auto table_id = getStorageID();
        // Check if at least one direct dependency is attached
        size_t dependencies_count = DatabaseCatalog::instance().getDependencies(table_id).size();
        if (dependencies_count)
        {
            auto start_time = std::chrono::steady_clock::now();

            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!stream_cancelled && num_created_consumers > 0)
            {
                if (!checkDependencies(table_id))
                    break;

                LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

                // Exit the loop & reschedule if some stream stalled
                auto some_stream_is_stalled = streamToViews();
                if (some_stream_is_stalled)
                {
                    LOG_TRACE(log, "Stream(s) stalled. Reschedule.");
                    break;
                }

                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts-start_time);
                if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
                {
                    LOG_TRACE(log, "Thread work duration limit exceeded. Reschedule.");
                    break;
                }
            }
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
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id);
    if (!table)
        throw Exception("Engine table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    const Settings & settings = global_context.getSettingsRef();
    size_t block_size = max_block_size;
    if (block_size == 0)
        block_size = settings.max_block_size;

    // Create a stream for each consumer and join them in a union stream
    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, kafka_context, false, true, true);
    auto block_io = interpreter.execute();

    // Create a stream for each consumer and join them in a union stream
    BlockInputStreams streams;
    streams.reserve(num_created_consumers);
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto stream
            = std::make_shared<KafkaBlockInputStream>(*this, kafka_context, block_io.out->getHeader().getNames(), block_size, false);
        streams.emplace_back(stream);

        // Limit read batch to maximum block size to allow DDL
        IBlockInputStream::LocalLimits limits;
        limits.speed_limits.max_execution_time = settings.stream_flush_interval_ms;
        limits.timeout_overflow_mode = OverflowMode::BREAK;
        stream->setLimits(limits);
    }

    // Join multiple streams if necessary
    BlockInputStreamPtr in;
    if (streams.size() > 1)
        in = std::make_shared<UnionBlockInputStream>(streams, nullptr, streams.size());
    else
        in = streams[0];

    // We can't cancel during copyData, as it's not aware of commits and other kafka-related stuff.
    // It will be cancelled on underlying layer (kafka buffer)
    std::atomic<bool> stub = {false};
    copyData(*in, *block_io.out, &stub);

    bool some_stream_is_stalled = false;
    for (auto & stream : streams)
    {
        some_stream_is_stalled = some_stream_is_stalled || stream->as<KafkaBlockInputStream>()->isStalled();
        stream->as<KafkaBlockInputStream>()->commit();
    }

    return some_stream_is_stalled;
}

void registerStorageKafka(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
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
          * - Do intermediate commits when the batch consumed and handled
          */

        // Check arguments and settings
        #define CHECK_KAFKA_STORAGE_ARGUMENT(ARG_NUM, PAR_NAME)            \
            /* One of the four required arguments is not specified */      \
            if (args_count < (ARG_NUM) && (ARG_NUM) <= 4 &&                    \
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
                args_count >= (ARG_NUM))                                     \
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
        CHECK_KAFKA_STORAGE_ARGUMENT(10, kafka_commit_every_batch)

        #undef CHECK_KAFKA_STORAGE_ARGUMENT

        // Get and check broker list
        String brokers = kafka_settings.kafka_broker_list;
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

        // Get and check topic list
        String topic_list = kafka_settings.kafka_topic_list.value;
        if (args_count >= 2)
        {
            engine_args[1] = evaluateConstantExpressionAsLiteral(engine_args[1], args.local_context);
            topic_list = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        }

        Names topics;
        boost::split(topics, topic_list , [](char c){ return c == ','; });
        for (String & topic : topics)
        {
            boost::trim(topic);
        }

        // Get and check group name
        String group = kafka_settings.kafka_group_name.value;
        if (args_count >= 3)
        {
            engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);
            group = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        }

        // Get and check message format name
        String format = kafka_settings.kafka_format.value;
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

        // Parse row delimiter (optional)
        char row_delimiter = kafka_settings.kafka_row_delimiter;
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
            else if (arg.empty())
            {
                row_delimiter = '\0';
            }
            else
            {
                row_delimiter = arg[0];
            }
        }

        // Parse format schema if supported (optional)
        String schema = kafka_settings.kafka_schema.value;
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

        // Parse number of consumers (optional)
        UInt64 num_consumers = kafka_settings.kafka_num_consumers;
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

        // Parse max block size (optional)
        UInt64 max_block_size = static_cast<size_t>(kafka_settings.kafka_max_block_size);
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

        size_t skip_broken = static_cast<size_t>(kafka_settings.kafka_skip_broken_messages);
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

        bool intermediate_commit = static_cast<bool>(kafka_settings.kafka_commit_every_batch);
        if (args_count >= 10)
        {
            const auto * ast = engine_args[9]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                intermediate_commit = static_cast<bool>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Flag for committing every batch must be 0 or 1", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        return StorageKafka::create(
            args.table_id, args.context, args.columns,
            brokers, group, topics, format, row_delimiter, schema, num_consumers, max_block_size, skip_broken, intermediate_commit);
    };

    factory.registerStorage("Kafka", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });
}

NamesAndTypesList StorageKafka::getVirtuals() const
{
    return NamesAndTypesList{
        {"_topic", std::make_shared<DataTypeString>()},
        {"_key", std::make_shared<DataTypeString>()},
        {"_offset", std::make_shared<DataTypeUInt64>()},
        {"_partition", std::make_shared<DataTypeUInt64>()},
        {"_timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())}
    };
}

}
