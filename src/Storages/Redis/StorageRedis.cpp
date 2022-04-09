#include <Storages/Redis/StorageRedis.h>
#include <Storages/Redis/parseSyslogLevel.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/Redis/RedisBlockOutputStream.h>
#include <Storages/Redis/RedisSettings.h>
#include <Storages/Redis/RedisStreamsSource.h>
#include <Storages/Redis/WriteBufferToRedisProducer.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <base/getFQDNOrHostName.h>
#include <base/logger_useful.h>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/config_version.h>
#include <Common/formatReadable.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int QUERY_NOT_ALLOWED;
}

namespace
{
    static const auto RESCHEDULE_MS = 500;
    static const auto BACKOFF_TRESHOLD = 32000;
    static const auto MAX_THREAD_WORK_DURATION_MS = 60000;
}

StorageRedis::StorageRedis(
    const StorageID & table_id_, ContextPtr context_,
    const ColumnsDescription & columns_, std::unique_ptr<RedisSettings> redis_settings_,
    const String & collection_name_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , redis_settings(std::move(redis_settings_))
    , streams(parseStreams(getContext()->getMacros()->expand(redis_settings->redis_stream_list.value)))
    , brokers(getContext()->getMacros()->expand(redis_settings->redis_broker_list.value))
    , group(getContext()->getMacros()->expand(redis_settings->redis_group_name.value))
    , client_id(
          redis_settings->redis_client_id.value.empty() ? getDefaultClientId(table_id_)
                                                        : getContext()->getMacros()->expand(redis_settings->redis_client_id.value))
    , num_consumers(redis_settings->redis_num_consumers.value)
    , log(&Poco::Logger::get("StorageRedis (" + table_id_.table_name + ")"))
    , semaphore(0, num_consumers)
    , intermediate_commit(redis_settings->redis_commit_every_batch.value)
    , settings_adjustments(createSettingsAdjustments())
    , thread_per_consumer(redis_settings->redis_thread_per_consumer.value)
    , collection_name(collection_name_)
    , milliseconds_to_wait(RESCHEDULE_MS)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    redis = std::make_shared<sw::redis::Redis>(brokers);
    /// TODO: should a create consumer groups?
    auto task_count = thread_per_consumer ? num_consumers : 1;
    try
    {
        for (size_t i = 0; i < task_count; ++i)
        {
            auto task = getContext()->getMessageBrokerSchedulePool().createTask(log->name(), [this, i]{ threadFunc(i); });
            task->deactivate();
            tasks.emplace_back(std::make_shared<TaskContext>(std::move(task)));
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

SettingsChanges StorageRedis::createSettingsAdjustments()
{
    SettingsChanges result;
    // Needed for backward compatibility
    if (!redis_settings->input_format_skip_unknown_fields.changed)
    {
        // Always skip unknown fields regardless of the context (JSON or TSKV)
        redis_settings->input_format_skip_unknown_fields = true;
    }

    if (!redis_settings->input_format_allow_errors_ratio.changed)
    {
        redis_settings->input_format_allow_errors_ratio = 0.;
    }

    for (const auto & setting : *redis_settings)
    {
        const auto & name = setting.getName();
        if (name.find("redis_") == std::string::npos)
            result.emplace_back(name, setting.getValue());
    }
    return result;
}

Names StorageRedis::parseStreams(String stream_list)
{
    Names result;
    boost::split(result,stream_list,[](char c){ return c == ','; });
    for (String & stream : result)
    {
        boost::trim(stream);
    }
    return result;
}

String StorageRedis::getDefaultClientId(const StorageID & table_id_)
{
    return fmt::format("{}-{}-{}-{}", VERSION_NAME, getFQDNOrHostName(), table_id_.database_name, table_id_.table_name);
}


Pipe StorageRedis::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /* query_info */,
    ContextPtr local_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    unsigned /* num_streams */)
{
    if (num_created_consumers == 0)
        return {};

    if (!local_context->getSettingsRef().stream_like_engine_allow_direct_select)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed. To enable use setting `stream_like_engine_allow_direct_select`");

    if (mv_attached)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageRedis with attached materialized views");

    /// Always use all consumers at once, otherwise SELECT may not read messages from all partitions.
    Pipes pipes;
    pipes.reserve(num_created_consumers);
    auto modified_context = Context::createCopy(local_context);
    modified_context->applySettingsChanges(settings_adjustments);

    // Claim as many consumers as requested, but don't block
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        /// Use block size of 1, otherwise LIMIT won't work properly as it will buffer excess messages in the last block
        /// TODO: probably that leads to awful performance.
        /// FIXME: seems that doesn't help with extra reading and committing unprocessed messages.
        pipes.emplace_back(std::make_shared<RedisStreamsSource>(*this, metadata_snapshot, modified_context, column_names, log, 1));
    }

    LOG_DEBUG(log, "Starting reading {} streams", pipes.size());
    return Pipe::unitePipes(std::move(pipes));
}


SinkToStoragePtr StorageRedis::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    auto modified_context = Context::createCopy(local_context);
    modified_context->applySettingsChanges(settings_adjustments);

    if (streams.size() > 1)
        throw Exception("Can't write to Redis table with multiple streams!", ErrorCodes::NOT_IMPLEMENTED);

    return std::make_shared<RedisSink>(*this, metadata_snapshot, modified_context);
}


void StorageRedis::startup()
{
    try
    {
        for (size_t i = 0; i < num_consumers; ++i)
        {
            pushReadBuffer(createReadBuffer(std::to_string(i)));
            ++num_created_consumers;
        }

        for (auto & task : tasks)
        {
            task->holder->activateAndSchedule();
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void StorageRedis::shutdown()
{
    for (auto & task : tasks)
    {
        // Interrupt streaming thread
        task->stream_cancelled = true;

        LOG_TRACE(log, "Waiting for cleanup");
        task->holder->deactivate();
    }

    for (size_t i = 0; i < num_created_consumers; ++i)
        auto buffer = popReadBuffer();
}


void StorageRedis::pushReadBuffer(ConsumerBufferPtr buffer)
{
    std::lock_guard lock(mutex);
    buffers.push_back(buffer);
    semaphore.set();
}


ConsumerBufferPtr StorageRedis::popReadBuffer()
{
    return popReadBuffer(std::chrono::milliseconds::zero());
}


ConsumerBufferPtr StorageRedis::popReadBuffer(std::chrono::milliseconds timeout)
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

ProducerBufferPtr StorageRedis::createWriteBuffer()
{
    return std::make_shared<WriteBufferToRedisProducer>(
        redis, streams[0], std::nullopt, 1, 1024);
}


ConsumerBufferPtr StorageRedis::createReadBuffer(const std::string& id)
{
    return std::make_shared<ReadBufferFromRedisConsumer>(redis, group, id, log, getPollMaxBatchSize(), getPollTimeoutMillisecond(), intermediate_commit, streams);
}

size_t StorageRedis::getMaxBlockSize() const
{
    return redis_settings->redis_max_block_size.changed
        ? redis_settings->redis_max_block_size.value
        : (getContext()->getSettingsRef().max_insert_block_size.value / num_consumers);
}

size_t StorageRedis::getPollMaxBatchSize() const
{
    size_t batch_size = redis_settings->redis_poll_max_batch_size.changed
                        ? redis_settings->redis_poll_max_batch_size.value
                        : getContext()->getSettingsRef().max_block_size.value;

    return std::min(batch_size,getMaxBlockSize());
}

size_t StorageRedis::getPollTimeoutMillisecond() const
{
    return redis_settings->redis_poll_timeout_ms.changed
        ? redis_settings->redis_poll_timeout_ms.totalMilliseconds()
        : getContext()->getSettingsRef().stream_poll_timeout_ms.totalMilliseconds();
}

bool StorageRedis::checkDependencies(const StorageID & table_id)
{
    // Check if all dependencies are attached
    auto dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    if (dependencies.empty())
        return true;

    // Check the dependencies are ready?
    for (const auto & db_tab : dependencies)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(db_tab, getContext());
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

void StorageRedis::threadFunc(size_t idx)
{
    assert(idx < tasks.size());
    auto task = tasks[idx];
    try
    {
        auto table_id = getStorageID();

        auto dependencies_count = DatabaseCatalog::instance().getDependencies(table_id).size();

        if (dependencies_count)
        {
            auto start_time = std::chrono::steady_clock::now();

            mv_attached.store(true);
            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!task->stream_cancelled)
            {
                if (!checkDependencies(table_id))
                {
                    /// For this case, we can not wait for watch thread to wake up
                    break;
                }

                LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

                if (streamToViews())
                {
                    if (milliseconds_to_wait < BACKOFF_TRESHOLD)
                        milliseconds_to_wait *= 2;
                    break;
                }
                else
                {
                    milliseconds_to_wait = RESCHEDULE_MS;
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

    mv_attached.store(false);

    // Wait for attached views
    if (!task->stream_cancelled)
    {
        task->holder->scheduleAfter(milliseconds_to_wait);
    }
}


bool StorageRedis::streamToViews()
{
    Stopwatch watch;

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception("Engine table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);
    auto metadata_snapshot = getInMemoryMetadataPtr();

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    size_t block_size = getMaxBlockSize();

    auto redis_context = Context::createCopy(getContext());
    redis_context->makeQueryContext();
    redis_context->applySettingsChanges(settings_adjustments);

    // Create a stream for each consumer and join them in a union stream
    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, redis_context, false, true, true);
    auto block_io = interpreter.execute();

    // Create a stream for each consumer and join them in a union stream
    std::vector<std::shared_ptr<RedisStreamsSource>> sources;
    Pipes pipes;

    auto stream_count = thread_per_consumer ? 1 : num_created_consumers;
    sources.reserve(stream_count);
    pipes.reserve(stream_count);
    for (size_t i = 0; i < stream_count; ++i)
    {
        auto source = std::make_shared<RedisStreamsSource>(*this, metadata_snapshot, redis_context, block_io.pipeline.getHeader().getNames(), log, block_size);
        sources.emplace_back(source);
        pipes.emplace_back(source);

        // Limit read batch to maximum block size to allow DDL
        StreamLocalLimits limits;

        limits.speed_limits.max_execution_time = redis_settings->redis_flush_interval_ms.changed
                                                 ? redis_settings->redis_flush_interval_ms
                                                 : getContext()->getSettingsRef().stream_flush_interval_ms;

        limits.timeout_overflow_mode = OverflowMode::BREAK;
        source->setLimits(limits);
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    // We can't cancel during copyData, as it's not aware of commits and other redis-related stuff.
    // It will be cancelled on underlying layer (redis buffer)

    size_t rows = 0;
    {
        block_io.pipeline.complete(std::move(pipe));
        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }

    bool some_stream_is_stalled = false;
    for (auto & source : sources)
    {
        some_stream_is_stalled = some_stream_is_stalled || source->isStalled();
        source->commit();
    }

    UInt64 milliseconds = watch.elapsedMilliseconds();
    LOG_DEBUG(log, "Pushing {} rows to {} took {} ms.",
        formatReadableQuantity(rows), table_id.getNameForLogs(), milliseconds);

    return some_stream_is_stalled;
}

void registerStorageRedis(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        size_t args_count = engine_args.size();
        bool has_settings = args.storage_def->settings;

        auto redis_settings = std::make_unique<RedisSettings>();
        auto named_collection = getExternalDataSourceConfiguration(args.engine_args, *redis_settings, args.getLocalContext());
        if (has_settings)
        {
            redis_settings->loadFromQuery(*args.storage_def);
        }

        // Check arguments and settings
        #define CHECK_REDIS_STORAGE_ARGUMENT(ARG_NUM, PAR_NAME, EVAL)       \
            /* One of the four required arguments is not specified */       \
            if (args_count < (ARG_NUM) && (ARG_NUM) <= 4 &&                 \
                !redis_settings->PAR_NAME.changed)                          \
            {                                                               \
                throw Exception(                                            \
                    "Required parameter '" #PAR_NAME "' "                   \
                    "for storage Redis not specified",                      \
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);          \
            }                                                               \
            if (args_count >= (ARG_NUM))                                    \
            {                                                               \
                /* The same argument is given in two places */              \
                if (has_settings &&                                         \
                    redis_settings->PAR_NAME.changed)                       \
                {                                                           \
                    throw Exception(                                        \
                        "The argument №" #ARG_NUM " of storage Redis "      \
                        "and the parameter '" #PAR_NAME "' "                \
                        "in SETTINGS cannot be specified at the same time", \
                        ErrorCodes::BAD_ARGUMENTS);                         \
                }                                                           \
                /* move engine args to settings */                          \
                else                                                        \
                {                                                           \
                    if ((EVAL) == 1)                                        \
                    {                                                       \
                        engine_args[(ARG_NUM)-1] =                          \
                            evaluateConstantExpressionAsLiteral(            \
                                engine_args[(ARG_NUM)-1],                   \
                                args.getLocalContext());                    \
                    }                                                       \
                    if ((EVAL) == 2)                                        \
                    {                                                       \
                        engine_args[(ARG_NUM)-1] =                          \
                           evaluateConstantExpressionOrIdentifierAsLiteral( \
                                engine_args[(ARG_NUM)-1],                   \
                                args.getLocalContext());                    \
                    }                                                       \
                    redis_settings->PAR_NAME =                              \
                        engine_args[(ARG_NUM)-1]->as<ASTLiteral &>().value; \
                }                                                           \
            }

        /** Arguments of engine is following:
          * - Redis broker list
          * - List of streams
          * - Group ID (may be a constaint expression with a string result)
          * - Message format (string)
          * - Row delimiter
          * - Schema (optional, if the format supports it)
          * - Number of consumers
          * - Max block size for background consumption
          * - Skip (at least) unreadable messages number
          * - Do intermediate commits when the batch consumed and handled
          */

        String collection_name;
        if (named_collection)
        {
            collection_name = assert_cast<const ASTIdentifier *>(args.engine_args[0].get())->name();
        }
        else
        {
            /* 0 = raw, 1 = evaluateConstantExpressionAsLiteral, 2=evaluateConstantExpressionOrIdentifierAsLiteral */
            CHECK_REDIS_STORAGE_ARGUMENT(1, redis_broker_list, 0)
            CHECK_REDIS_STORAGE_ARGUMENT(2, redis_stream_list, 1)
            CHECK_REDIS_STORAGE_ARGUMENT(3, redis_group_name, 2)
            CHECK_REDIS_STORAGE_ARGUMENT(7, redis_num_consumers, 0)
            CHECK_REDIS_STORAGE_ARGUMENT(8, redis_max_block_size, 0)
            CHECK_REDIS_STORAGE_ARGUMENT(10, redis_commit_every_batch, 0)
        }

        #undef CHECK_REDIS_STORAGE_ARGUMENT

        auto num_consumers = redis_settings->redis_num_consumers.value;
        auto physical_cpu_cores = getNumberOfPhysicalCPUCores();

        if (num_consumers > physical_cpu_cores)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of consumers can not be bigger than {}", physical_cpu_cores);
        }
        else if (num_consumers < 1)
        {
            throw Exception("Number of consumers can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        if (redis_settings->redis_max_block_size.changed && redis_settings->redis_max_block_size.value < 1)
        {
            throw Exception("redis_max_block_size can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        if (redis_settings->redis_poll_max_batch_size.changed && redis_settings->redis_poll_max_batch_size.value < 1)
        {
            throw Exception("redis_poll_max_batch_size can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        return StorageRedis::create(args.table_id, args.getContext(), args.columns, std::move(redis_settings), collection_name);
    };
    factory.registerStorage("Redis", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });
}

NamesAndTypesList StorageRedis::getVirtuals() const
{
    auto result = NamesAndTypesList{
        {"_stream", std::make_shared<DataTypeString>()},
        {"_key", std::make_shared<DataTypeString>()},
        {"_timestamp", std::make_shared<DataTypeUInt64>()},
        {"_sequence_number", std::make_shared<DataTypeUInt64>()}
    };
    return result;
}

Names StorageRedis::getVirtualColumnNames() const
{
    auto result = Names {
        "_stream",
        "_key",
        "_timestamp",
        "_sequence_number"
    };
    return result;
}

}
