#include <Storages/StorageQueryRunner.h>

#include <Access/Common/AccessFlags.h>
#include <Access/DefinerDependencies.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSQLSecurity.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/QueryRunnerSettings.h>
#include <Storages/StorageFactory.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/LoggingHelpers.h>
#include <Common/QueryScope.h>
#include <Common/SettingsChanges.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <unordered_set>


namespace CurrentMetrics
{
    extern const Metric QueryRunnerThreads;
    extern const Metric QueryRunnerThreadsActive;
    extern const Metric QueryRunnerThreadsScheduled;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 distributed_connections_pool_size;
    extern const SettingsLoadBalancing load_balancing;
    extern const SettingsString log_comment;
    extern const SettingsBool log_queries;
    extern const SettingsMilliseconds log_queries_min_query_duration_ms;
    extern const SettingsLogQueriesType log_queries_min_type;
    extern const SettingsBool log_query_settings;
}

namespace QueryRunnerSetting
{
    extern const QueryRunnerSettingsString cluster;
    extern const QueryRunnerSettingsUInt64 max_queue_size;
    extern const QueryRunnerSettingsQueryRunnerMode mode;
    extern const QueryRunnerSettingsString shard;
    extern const QueryRunnerSettingsUInt64 threads;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    const String QUERY_COLUMN = "query";
    const String DATABASE_COLUMN = "database";
    const String SETTINGS_COLUMN = "settings";

    enum class ShardSelectorKind : uint8_t
    {
        Fixed,
        Random,
        All,
    };

    struct ShardSelector
    {
        ShardSelectorKind kind;
        UInt64 fixed_shard_num;
    };

    ShardSelector parseShardSelector(const String & value)
    {
        if (value == "random")
            return {ShardSelectorKind::Random, 0};
        if (value == "all")
            return {ShardSelectorKind::All, 0};

        UInt64 shard_num = 0;
        if (!tryParse<UInt64>(shard_num, value) || shard_num < 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The 'shard' setting of the QueryRunner engine must be a positive integer, 'random' or 'all', got '{}'",
                value);

        return {ShardSelectorKind::Fixed, shard_num};
    }
}

struct QueryRunnerJobOrigin
{
    std::optional<UUID> user_id;
    std::optional<std::vector<UUID>> roles;
    String current_user;
    String initial_user;
    String authenticated_user;
};

class PrefixLatch
{
public:
    /// Wait for all of [0, next_seq) to retire.
    void waitForAllIssued(const QueryStatusPtr & query_status)
    {
        std::unique_lock l(mutex);
        const UInt64 threshold = next_seq;
        while (!cv.wait_for(l, poll_interval, [&] { return in_progress.empty() || *in_progress.begin() >= threshold; }))
        {
            if (query_status)
            {
                query_status->checkTimeLimit();
            }
        }
    }

    UInt64 issue()
    {
        std::lock_guard l(mutex);
        UInt64 result = next_seq++;
        in_progress.insert(result);
        return result;
    }

    void retire(UInt64 seq)
    {
        {
            std::lock_guard l(mutex);
            in_progress.erase(seq);
        }
        cv.notify_all();
    }

private:
    static constexpr std::chrono::milliseconds poll_interval{100};
    std::mutex mutex;
    std::condition_variable cv;
    UInt64 next_seq = 0;
    std::set<UInt64> in_progress;
};

class CountDownLatch
{
public:
    explicit CountDownLatch(size_t remaining_) : remaining(remaining_) {}

    void countDown()
    {
        {
            std::lock_guard lock(mutex);
            chassert(remaining > 0);
            if (--remaining > 0)
                return;
        }
        cv.notify_all();
    }

    void wait(const QueryStatusPtr & query_status)
    {
        std::unique_lock lock(mutex);
        while (!cv.wait_for(lock, poll_interval, [this] { return remaining == 0; }))
        {
            if (query_status)
            {
                query_status->checkTimeLimit();
            }
        }
    }

private:
    static constexpr std::chrono::milliseconds poll_interval{100};

    std::mutex mutex;
    std::condition_variable cv;
    size_t remaining;
};

struct QueryRunnerJob
{
    String query;
    String database;
    SettingsChanges settings_changes;
    std::shared_ptr<const QueryRunnerJobOrigin> origin;
    std::shared_ptr<CountDownLatch> batch;
    UInt64 seq = 0;
};

/// Used to cancel the remote queries and unblock the dispatcher's workers on shutdown.
/// Cancelling is required because otherwise server shutdown would be blocked on
/// queries being executed on other clusters - arbitrarily long.
class RemoteQueryExecutorRegistry
{
public:
    bool tryAdd(RemoteQueryExecutor * executor)
    {
        std::lock_guard lock(mutex);
        if (finished)
            return false;
        executors.insert(executor);
        return true;
    }

    void remove(RemoteQueryExecutor * executor)
    {
        std::lock_guard lock(mutex);
        executors.erase(executor);
    }

    void cancelAll()
    {
        std::lock_guard lock(mutex);
        finished = true;
        for (auto * executor : executors)
        {
            try
            {
                executor->cancel();
            }
            catch (...)
            {
                tryLogCurrentException(getLogger("QueryRunner"), "Failed to cancel a remote query executor");
            }
        }
    }

private:
    std::mutex mutex;
    bool finished TSA_GUARDED_BY(mutex) = false;
    std::unordered_set<RemoteQueryExecutor *> executors TSA_GUARDED_BY(mutex);
};

class RegisteredRemoteQueryExecutor
{
public:
    template <typename... Args>
    static std::optional<RegisteredRemoteQueryExecutor> tryCreate(RemoteQueryExecutorRegistry & registry, Args &&... args)
    {
        auto executor = std::make_unique<RemoteQueryExecutor>(std::forward<Args>(args)...);
        if (!registry.tryAdd(executor.get()))
            return std::nullopt;
        return RegisteredRemoteQueryExecutor(registry, std::move(executor));
    }

    RegisteredRemoteQueryExecutor(RegisteredRemoteQueryExecutor &&) = default;

    ~RegisteredRemoteQueryExecutor()
    {
        if (executor)
            registry.remove(executor.get());
    }

    RemoteQueryExecutor * operator->() const { return executor.get(); }

private:
    RegisteredRemoteQueryExecutor(RemoteQueryExecutorRegistry & registry_, std::unique_ptr<RemoteQueryExecutor> executor_)
        : registry(registry_), executor(std::move(executor_))
    {
    }

    RemoteQueryExecutorRegistry & registry;
    std::unique_ptr<RemoteQueryExecutor> executor;
};

class QueryRunnerDispatcher : WithContext
{
public:
    QueryRunnerDispatcher(
        ContextPtr global_context_,
        const String & cluster_name_,
        ShardSelector shard_selector_,
        UInt64 num_threads_,
        UInt64 max_queue_size_,
        LoggerPtr log_)
        : WithContext(global_context_)
        , cluster_name(cluster_name_)
        , shard_selector(shard_selector_)
        , queue(max_queue_size_)
        , num_threads(num_threads_)
        , max_queue_size(max_queue_size_)
        , log(log_)
        , pool(CurrentMetrics::QueryRunnerThreads, CurrentMetrics::QueryRunnerThreadsActive, CurrentMetrics::QueryRunnerThreadsScheduled, num_threads_)
    {
        client_info.client_name = String(client_name);
        client_info.setInitialQuery();
    }

    void start()
    {
        try
        {
            for (size_t i = 0; i < num_threads; ++i)
                pool.scheduleOrThrowOnError([this] { workerLoop(); });
        }
        catch (...)
        {
            shutdown();
            throw;
        }
    }

    void submit(QueryRunnerJob job)
    {
        job.seq = pending.issue();
        const auto batch = job.batch;
        const UInt64 seq = job.seq;
        try
        {
            if (queue.tryPush(std::move(job)))
                return;
        }
        catch (...)
        {
            finishJob(batch, seq);
            throw;
        }

        if (queue.isFinished())
            LOG_WARNING(log, "The table is shutting down, discarding the query");
        else
            LOG_ERROR(LogFrequencyLimiter(log, 5), "The queue is full (max_queue_size = {}), discarding the query", max_queue_size);
        finishJob(batch, seq);
    }

    void waitForAllPending(const QueryStatusPtr & query_status)
    {
        pending.waitForAllIssued(query_status);
    }

    void shutdown()
    {
        if (shutdown_called.exchange(true))
            return;

        queue.finish();

        QueryRunnerJob job;
        while (queue.tryPop(job))
            finishJob(job.batch, job.seq);

        cluster_executors.cancelAll();
        pool.wait();
    }

private:
    void finishJob(const std::shared_ptr<CountDownLatch> & batch, UInt64 seq)
    {
        if (batch)
            batch->countDown();
        pending.retire(seq);
    }

    void workerLoop()
    {
        setThreadName(ThreadName::QUERY_RUNNER);

        QueryRunnerJob job;
        while (queue.pop(job))
        {
            try
            {
                executeJob(job);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to execute a query");
            }

            finishJob(job.batch, job.seq);
        }
    }

    void executeJob(const QueryRunnerJob & job)
    {
        if (shutdown_called)
            return;

        auto job_context = makeJobContext(job);
        QueryScope query_scope = QueryScope::create(job_context);

        if (cluster_name.empty())
            executeLocally(job, job_context);
        else
            executeOnCluster(job, job_context);
    }

    ContextMutablePtr makeJobContext(const QueryRunnerJob & job) const
    {
        auto job_context = Context::createCopy(getContext());
        job_context->makeQueryContext();
        job_context->setClientInfo(client_info);

        if (job.origin->user_id)
        {
            chassert(cluster_name.empty());
            job_context->setUser(*job.origin->user_id);
        }
        if (job.origin->roles)
        {
            chassert(cluster_name.empty());
            job_context->setCurrentRoles(*job.origin->roles);
        }

        job_context->setCurrentUserName(job.origin->current_user);
        job_context->setInitialUserName(job.origin->initial_user);
        job_context->setAuthenticatedUserName(job.origin->authenticated_user);

        if (cluster_name.empty() && !job.database.empty())
            job_context->setCurrentDatabase(job.database);

        job_context->setCurrentQueryId({});

        if (!job.settings_changes.empty())
        {
            /// In the cluster mode, the settings constraints are checked by the destination cluster.
            if (cluster_name.empty())
                job_context->checkSettingsConstraints(job.settings_changes, SettingSource::QUERY);
            job_context->applySettingsChanges(job.settings_changes);
        }

        /// The engine always discards query results, so there is no point in transferring them over the network.
        job_context->setSetting("discard_query_data", true);

        return job_context;
    }

    void executeLocally(const QueryRunnerJob & job, ContextMutablePtr job_context) const
    {
        auto io = executeQuery(job.query, job_context, QueryFlags{ .internal = true }).second;
        try
        {
            if (io.pipeline.initialized())
            {
                if (io.pipeline.pulling())
                {
                    PullingPipelineExecutor executor(io.pipeline);
                    Block block;
                    while (executor.pull(block))
                        ;
                }
                else if (io.pipeline.completed())
                {
                    CompletedPipelineExecutor executor(io.pipeline);
                    executor.execute();
                }
                else
                {
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The `QueryRunner` engine does not support this query: {}", job.query);
                }
            }
            io.onFinish();
        }
        catch (...)
        {
            io.onException();
            throw;
        }
    }

    ConnectionPoolWithFailoverPtr getPool(UInt64 shard_num, const String & database)
    {
        std::lock_guard lock(pools_mutex);
        if (auto it = pools.find({shard_num, database}); it != pools.end())
            return it->second;

        const auto cluster = getContext()->getCluster(cluster_name);
        const auto & addresses = cluster->getShardsAddresses().at(shard_num - 1);
        const auto & settings = getContext()->getSettingsRef();

        ConnectionPoolPtrs replica_pools;
        replica_pools.reserve(addresses.size());
        for (const auto & address : addresses)
            replica_pools.push_back(ConnectionPoolFactory::instance().get(
                static_cast<unsigned>(settings[Setting::distributed_connections_pool_size]),
                address.host_name,
                address.port,
                database.empty() ? address.default_database : database,
                address.user,
                address.password,
                address.proto_send_chunked,
                address.proto_recv_chunked,
                address.quota_key,
                address.cluster,
                address.cluster_secret,
                String(client_name),
                address.compression,
                address.secure,
                address.bind_host,
                address.priority));

        const auto connection_pool = std::make_shared<ConnectionPoolWithFailover>(std::move(replica_pools), settings[Setting::load_balancing]);
        pools.emplace(std::pair{shard_num, database}, connection_pool);
        return connection_pool;
    }

    void executeOnCluster(const QueryRunnerJob & job, ContextMutablePtr job_context)
    {
        if (shard_selector.kind == ShardSelectorKind::Fixed)
        {
            executeOnShard(shard_selector.fixed_shard_num, job, job_context);
            return;
        }

        const size_t num_shards = getContext()->getCluster(cluster_name)->getShardsInfo().size();
        if (shard_selector.kind == ShardSelectorKind::Random)
        {
            std::uniform_int_distribution<UInt64> distribution(1, num_shards);
            executeOnShard(distribution(thread_local_rng), job, job_context);
            return;
        }

        for (UInt64 shard = 1; shard <= num_shards; ++shard)
        {
            try
            {
                executeOnShard(shard, job, job_context);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to execute a query");
            }
        }
    }

    void executeOnShard(UInt64 shard_num, const QueryRunnerJob & job, ContextMutablePtr job_context)
    {
        const auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(job_context->getSettingsRef());
        auto connection = getPool(shard_num, job.database)->get(timeouts, getContext()->getSettingsRef(), /*force_connected=*/ true);

        auto registered = RegisteredRemoteQueryExecutor::tryCreate(cluster_executors, *connection, job.query, std::make_shared<const Block>(), job_context);
        if (!registered)
            return;
        auto & executor = *registered;

        const auto query_start_time = std::chrono::system_clock::now();
        Stopwatch watch;

        logClusterQuery(job, job_context, query_start_time, QueryLogElementType::QUERY_START, 0);

        try
        {
            executor->sendQuery(ClientInfo::QueryKind::INITIAL_QUERY);

            while (!executor->readBlock().empty())
                ;

            executor->finish();
        }
        catch (...)
        {
            logClusterQuery(job, job_context, query_start_time, QueryLogElementType::EXCEPTION_WHILE_PROCESSING, watch.elapsedMilliseconds());
            throw;
        }

        if (!executor->isCancelled())
            logClusterQuery(job, job_context, query_start_time, QueryLogElementType::QUERY_FINISH, watch.elapsedMilliseconds());
    }

    void logClusterQuery(
        const QueryRunnerJob & job,
        ContextMutablePtr job_context,
        const std::chrono::system_clock::time_point query_start_time,
        const QueryLogElementType type,
        const UInt64 duration_ms) const
    {
        const auto & settings = job_context->getSettingsRef();
        if (!settings[Setting::log_queries] || type < settings[Setting::log_queries_min_type])
            return;

        const UInt64 min_duration_ms = settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds();
        if (duration_ms < min_duration_ms)
            return;

        auto query_log = getContext()->getQueryLog();
        if (!query_log)
            return;

        const auto event_time = std::chrono::system_clock::now();

        QueryLogElement elem;
        elem.type = type;
        elem.event_time = timeInSeconds(event_time);
        elem.event_time_microseconds = timeInMicroseconds(event_time);
        elem.query_start_time = timeInSeconds(query_start_time);
        elem.query_start_time_microseconds = timeInMicroseconds(query_start_time);
        elem.query_duration_ms = duration_ms;
        elem.query = job.query;
        elem.current_database = job.database;
        elem.log_comment = settings[Setting::log_comment];
        elem.client_info = job_context->getClientInfo();
        elem.is_internal = true;

        if (settings[Setting::log_query_settings])
            elem.query_settings = std::make_shared<Settings>(settings);

        if (type == QueryLogElementType::EXCEPTION_WHILE_PROCESSING)
        {
            elem.exception_code = getCurrentExceptionCode();
            elem.exception = getCurrentExceptionMessage(false);
        }

        query_log->add(std::move(elem));
    }

    static constexpr std::string_view client_name = "QueryRunner";

    const String cluster_name;
    const ShardSelector shard_selector;
    ClientInfo client_info;
    ConcurrentBoundedQueue<QueryRunnerJob> queue;
    const size_t num_threads;
    const size_t max_queue_size;
    LoggerPtr log;
    ThreadPool pool;

    std::atomic<bool> shutdown_called = false;

    PrefixLatch pending;

    std::mutex pools_mutex;
    std::map<std::pair<UInt64, String>, ConnectionPoolWithFailoverPtr> pools TSA_GUARDED_BY(pools_mutex);

    RemoteQueryExecutorRegistry cluster_executors;
};


class QueryRunnerSink : public SinkToStorage
{
public:
    QueryRunnerSink(
        SharedHeader header,
        QueryRunnerDispatcher & dispatcher_,
        bool synchronous_,
        std::shared_ptr<const QueryRunnerJobOrigin> origin_,
        QueryStatusPtr query_status_)
        : SinkToStorage(header)
        , dispatcher(dispatcher_)
        , synchronous(synchronous_)
        , origin(std::move(origin_))
        , query_status(std::move(query_status_))
    {
    }

    String getName() const override { return "QueryRunnerSink"; }

    void consume(Chunk & chunk) override
    {
        const size_t rows = chunk.getNumRows();
        if (!rows)
            return;

        const Block block = getHeader().cloneWithColumns(chunk.getColumns());
        const auto get_column = [&](const String & name) -> ColumnPtr
        {
            return block.has(name) ? block.getByName(name).column : nullptr;
        };

        const ColumnPtr query_column = get_column(QUERY_COLUMN);
        const ColumnPtr database_column = get_column(DATABASE_COLUMN);
        const ColumnPtr settings_column = get_column(SETTINGS_COLUMN);

        std::shared_ptr<CountDownLatch> batch;
        if (synchronous)
            batch = std::make_shared<CountDownLatch>(rows);

        for (size_t i = 0; i < rows; ++i)
        {
            QueryRunnerJob job;
            job.query = String(assert_cast<const ColumnString &>(*query_column).getDataAt(i));
            if (database_column)
                job.database = String(assert_cast<const ColumnString &>(*database_column).getDataAt(i));

            if (settings_column)
            {
                Field map_field;
                settings_column->get(i, map_field);
                for (const auto & entry : map_field.safeGet<Map>())
                {
                    const auto & pair = entry.safeGet<Tuple>();
                    job.settings_changes.emplace_back(pair.at(0).safeGet<String>(), pair.at(1));
                }
            }

            job.origin = origin;
            job.batch = batch;

            dispatcher.submit(std::move(job));
        }

        if (batch)
            batch->wait(query_status);
    }

private:
    QueryRunnerDispatcher & dispatcher;
    const bool synchronous;
    std::shared_ptr<const QueryRunnerJobOrigin> origin;
    QueryStatusPtr query_status;
};


StorageQueryRunner::StorageQueryRunner(
    const StorageID & table_id_,
    ColumnsDescription columns_,
    ConstraintsDescription constraints_,
    const String & comment,
    const ASTPtr & sql_security_,
    const QueryRunnerSettings & settings,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , mode(settings[QueryRunnerSetting::mode])
    , log(getLogger("StorageQueryRunner (" + table_id_.getFullTableName() + ")"))
{
    const String & cluster_name = settings[QueryRunnerSetting::cluster];

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(std::move(columns_));
    storage_metadata.setConstraints(std::move(constraints_));
    storage_metadata.setComment(comment);

    if (sql_security_)
    {
        if (!cluster_name.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "SQL SECURITY and DEFINER have no effect in the cluster mode and cannot be used together with the 'cluster' setting of the QueryRunner engine");

        storage_metadata.setSQLSecurity(sql_security_->as<ASTSQLSecurity &>());

        if (storage_metadata.sql_security_type == SQLSecurityType::DEFINER)
            DefinerDependencies::instance().addDependency(*storage_metadata.definer, table_id_);
    }
    else if (cluster_name.empty())
        storage_metadata.sql_security_type = SQLSecurityType::INVOKER;

    setInMemoryMetadata(storage_metadata);

    dispatcher = std::make_unique<QueryRunnerDispatcher>(
        getContext(),
        cluster_name,
        parseShardSelector(settings[QueryRunnerSetting::shard]),
        settings[QueryRunnerSetting::threads],
        settings[QueryRunnerSetting::max_queue_size],
        log);
}

StorageQueryRunner::~StorageQueryRunner() = default;

void StorageQueryRunner::startup()
{
    dispatcher->start();
}

void StorageQueryRunner::shutdown(bool /*is_drop*/)
{
    dispatcher->shutdown();
}

void StorageQueryRunner::waitForQueriesToFinish(const QueryStatusPtr & query_status)
{
    dispatcher->waitForAllPending(query_status);
}

SinkToStoragePtr StorageQueryRunner::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    const auto & inserter = local_context->getClientInfo();
    std::shared_ptr<const QueryRunnerJobOrigin> origin;
    if (!metadata_snapshot->sql_security_type)
    {
        local_context->checkAccess(AccessType::READ | AccessType::WRITE, toStringSource(AccessTypeObjects::Source::REMOTE));
        origin = std::make_shared<const QueryRunnerJobOrigin>(QueryRunnerJobOrigin{
            .user_id = {},
            .roles = {},
            .current_user = {},
            .initial_user = {},
            .authenticated_user = inserter.authenticated_user,
        });
    }
    else
    {
        switch (*metadata_snapshot->sql_security_type)
        {
            case SQLSecurityType::INVOKER:
                origin = std::make_shared<const QueryRunnerJobOrigin>(QueryRunnerJobOrigin{
                    .user_id = local_context->getUserID(),
                    .roles = local_context->getCurrentRoles(),
                    .current_user = inserter.current_user,
                    .initial_user = inserter.initial_user,
                    .authenticated_user = inserter.authenticated_user,
                });
                break;
            case SQLSecurityType::DEFINER:
                origin = std::make_shared<const QueryRunnerJobOrigin>(QueryRunnerJobOrigin{
                    .user_id = metadata_snapshot->getDefinerID(local_context),
                    .roles = {},
                    .current_user = *metadata_snapshot->definer,
                    .initial_user = *metadata_snapshot->definer,
                    .authenticated_user = inserter.authenticated_user,
                });
                break;
            case SQLSecurityType::NONE:
                origin = std::make_shared<const QueryRunnerJobOrigin>(QueryRunnerJobOrigin{
                    .user_id = {},
                    .roles = {},
                    .current_user = {},
                    .initial_user = {},
                    .authenticated_user = inserter.authenticated_user,
                });
                break;
        }
    }

    return std::make_shared<QueryRunnerSink>(
        std::make_shared<const Block>(metadata_snapshot->getSampleBlock()),
        *dispatcher,
        mode == QueryRunnerMode::SYNCHRONOUS,
        std::move(origin),
        local_context->getProcessListElement());
}

void StorageQueryRunner::drop()
{
    auto metadata_snapshot = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    if (metadata_snapshot->sql_security_type == SQLSecurityType::DEFINER)
        DefinerDependencies::instance().removeDependencies(getStorageID());
}


static void validateColumns(const ColumnsDescription & columns)
{
    bool has_query = false;

    for (const auto & column : columns)
    {
        if (column.default_desc.kind != ColumnDefaultKind::Default)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The '{}' column of a QueryRunner table cannot be {}",
                column.name,
                toString(column.default_desc.kind));

        if (column.name == QUERY_COLUMN)
        {
            if (!isString(column.type))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'query' column of a QueryRunner table must have type String, got {}", column.type->getName());
            has_query = true;
        }
        else if (column.name == DATABASE_COLUMN)
        {
            if (!isString(column.type))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'database' column of a QueryRunner table must have type String, got {}", column.type->getName());
        }
        else if (column.name == SETTINGS_COLUMN)
        {
            const auto * map_type = typeid_cast<const DataTypeMap *>(column.type.get());
            if (!map_type || !isString(removeLowCardinality(map_type->getKeyType())) || !isString(map_type->getValueType()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'settings' column of a QueryRunner table must have type Map(String, String), got {}", column.type->getName());
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected column '{}': a QueryRunner table allows only the columns "
                "'query String', 'database String', 'settings Map(String, String)'",
                column.name);
        }
    }

    if (!has_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A QueryRunner table requires the 'query String' column");
}

void registerStorageQueryRunner(StorageFactory & factory);
void registerStorageQueryRunner(StorageFactory & factory)
{
    factory.registerStorage("QueryRunner", [](const StorageFactory::Arguments & args) -> StoragePtr
    {
        if (!args.engine_args.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The QueryRunner engine does not take arguments, use the SETTINGS clause instead");

        QueryRunnerSettings settings;
        settings.loadFromQuery(*args.storage_def);

        const UInt64 num_threads = settings[QueryRunnerSetting::threads];
        if (num_threads < 1 || num_threads > 1024)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'threads' setting of the QueryRunner engine must be in the range [1, 1024], got {}", num_threads);

        const UInt64 max_queue_size = settings[QueryRunnerSetting::max_queue_size];
        if (max_queue_size < 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'max_queue_size' setting of the QueryRunner engine must be at least 1");

        const String & cluster_name = settings[QueryRunnerSetting::cluster];
        const ShardSelector shard_selector = parseShardSelector(settings[QueryRunnerSetting::shard]);

        if (cluster_name.empty())
        {
            if (settings[QueryRunnerSetting::shard].changed)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'shard' setting of the QueryRunner engine requires the 'cluster' setting");
        }
        else if (args.mode <= LoadingStrictnessLevel::CREATE)
        {
            args.getLocalContext()->checkAccess(AccessType::READ | AccessType::WRITE, toStringSource(AccessTypeObjects::Source::REMOTE));
            auto cluster = args.getContext()->getCluster(cluster_name);
            if (shard_selector.kind == ShardSelectorKind::Fixed && shard_selector.fixed_shard_num > cluster->getShardsInfo().size())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The 'shard' setting of the QueryRunner engine must be in the range [1, {}] for cluster '{}', got {}",
                    cluster->getShardsInfo().size(), cluster_name, shard_selector.fixed_shard_num);
        }

        validateColumns(args.columns);

        return std::make_shared<StorageQueryRunner>(
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            args.query.sql_security,
            settings,
            args.getContext());
    },
    {
        .supports_settings = true,
        .supports_parallel_insert = true,
        .supports_sql_security = true,
        .has_builtin_setting_fn = QueryRunnerSettings::hasBuiltin,
    });
}

}
