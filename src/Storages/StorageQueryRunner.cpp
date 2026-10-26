#include <Storages/StorageQueryRunner.h>

#include <Access/Common/AccessFlags.h>
#include <Access/DefinerDependencies.h>
#include <Client/Connection.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Client/SecondaryQuerySettings.h>
#include <Columns/ColumnString.h>
#include <Core/Protocol.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <Formats/FormatFactory.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSQLSecurity.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/QueryRunnerSettings.h>
#include <Storages/StorageFactory.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/LoggingHelpers.h>
#include <Common/ProfileEvents.h>
#include <Common/QueryScope.h>
#include <Common/SettingsChanges.h>
#include <Common/SilkFiberMutex.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>

#include <atomic>
#include <condition_variable>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <unordered_set>

#include "config.h"

#if USE_SILK
#include <Client/SilkConnectionPool.h>
#include <IO/SilkStreamSocketFactory.h>
#include <Common/SilkFiberScheduler.h>

#include <silk/fibers/future.h>
#endif


namespace CurrentMetrics
{
    extern const Metric QueryRunnerPendingQueries;
    extern const Metric QueryRunnerThreads;
    extern const Metric QueryRunnerThreadsActive;
    extern const Metric QueryRunnerThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event DistributedShardsSkipped;
}

namespace DB
{

namespace Setting
{
    extern const SettingsURI format_avro_schema_registry_url;
    extern const SettingsLoadBalancing load_balancing;
    extern const SettingsString log_comment;
    extern const SettingsBool log_queries;
    extern const SettingsMilliseconds log_queries_min_query_duration_ms;
    extern const SettingsLogQueriesType log_queries_min_type;
    extern const SettingsBool log_query_settings;
    extern const SettingsBool skip_unavailable_shards;
    extern const SettingsSkipUnavailableShardsMode skip_unavailable_shards_mode;
}

namespace QueryRunnerSetting
{
    extern const QueryRunnerSettingsString cluster;
    extern const QueryRunnerSettingsUInt64 connection_pool_size;
    extern const QueryRunnerSettingsUInt64 max_pending_queries;
    extern const QueryRunnerSettingsQueryRunnerMode mode;
    extern const QueryRunnerSettingsWorkerKind scheduler;
    extern const QueryRunnerSettingsString shard;
    extern const QueryRunnerSettingsNonZeroUInt64 threads;
}

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int SYSTEM_ERROR;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int UNKNOWN_TABLE;
    extern const int USER_EXPIRED;
#if USE_SILK
    extern const int CANNOT_SCHEDULE_TASK;
#endif
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
    /// External (pushed) roles of the originating session. Carried over and re-applied via `setUser`
    /// so a role that exists only as an external role is not lost or rejected with
    /// `SET_NON_GRANTED_ROLE` when the deferred job rebuilds the context. Empty in the DEFINER/NONE
    /// cases, which intentionally run as a different (or no) principal.
    std::vector<UUID> external_roles;
    /// Credential grant limit of the originating session (null if the session is not limited).
    /// Carried over so a limited credential does not regain full rights when the deferred job runs
    /// under a freshly-built context. Only meaningful in the INVOKER case; the DEFINER/NONE cases
    /// intentionally run as a different (or no) principal, so it stays null there.
    std::shared_ptr<const AccessRightsElements> authentication_grants;
    /// Expiry (VALID UNTIL) of the authenticating method of the originating session, 0 if none.
    /// Carried over so the deferred job fails closed if the credential has expired between the
    /// insert and the moment the job runs. The synchronous path re-checks expiry per query in
    /// `Session::checkIfUserIsStillValid`, but the deferred job has no session, so it must re-check
    /// here. Only meaningful in the INVOKER case; 0 (no expiry) in the DEFINER/NONE cases.
    time_t authentication_valid_until = 0;
    String current_user;
    String initial_user;
    String authenticated_user;
    Settings settings;
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

    template <typename Rep, typename Period>
    bool wait(const std::chrono::duration<Rep, Period> & timeout)
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, timeout, [this] { return remaining == 0; });
    }

private:
    std::mutex mutex;
    std::condition_variable cv;
    size_t remaining;
};

class QueryRunnerJobCompletion
{
public:
    QueryRunnerJobCompletion(PrefixLatch & pending_, std::shared_ptr<CountDownLatch> batch_)
        : pending(pending_), batch(std::move(batch_)), seq(pending.issue())
    {
    }

    ~QueryRunnerJobCompletion()
    {
        if (batch)
            batch->countDown();
        pending.retire(seq);
    }

private:
    PrefixLatch & pending;
    const std::shared_ptr<CountDownLatch> batch;
    const UInt64 seq;
    CurrentMetrics::Increment pending_queries_increment{CurrentMetrics::QueryRunnerPendingQueries};
};

struct QueryRunnerJob
{
    String query;
    String database;
    SettingsChanges settings_changes;
    std::shared_ptr<const QueryRunnerJobOrigin> origin;
    std::shared_ptr<QueryRunnerJobCompletion> completion;
};

static bool shouldSkipShard(const Settings & settings, int exception_code, bool received_data)
{
    if (!settings[Setting::skip_unavailable_shards])
        return false;

    if (exception_code == ErrorCodes::ALL_CONNECTION_TRIES_FAILED)
        return true;

    if (exception_code == ErrorCodes::LOGICAL_ERROR)
        return false;

    switch (settings[Setting::skip_unavailable_shards_mode].value)
    {
        case SkipUnavailableShardsMode::UNAVAILABLE:
            return false;
        case SkipUnavailableShardsMode::UNAVAILABLE_OR_TABLE_MISSING:
            return exception_code == ErrorCodes::UNKNOWN_TABLE || exception_code == ErrorCodes::UNKNOWN_DATABASE;
        case SkipUnavailableShardsMode::UNAVAILABLE_OR_EXCEPTION_BEFORE_PROCESSING:
            return !received_data;
    }
}

class QueryRunnerDispatcher : WithContext
{
    class RemoteQueryExecutor
    {
    public:
        RemoteQueryExecutor(IConnectionPool::Entry connection_, const QueryRunnerJob & job, ContextPtr context_)
            : connection(std::move(connection_))
            , query(job.query)
            , context(std::move(context_))
            , timeouts(ConnectionTimeouts::getTCPTimeoutsWithFailover(job.origin->settings))
        {
        }

        ~RemoteQueryExecutor()
        {
            std::lock_guard lock(mutex);
            if (sent_query && !finished)
                connection->disconnect();
        }

        void execute()
        {
            {
                std::lock_guard lock(mutex);
                if (was_cancelled)
                    return;

                ClientInfo query_client_info = context->getClientInfo();
                query_client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
                Settings settings = context->getSettingsRef();
                prepareSecondaryQuerySettings(settings);
                connection->sendQuery(
                    timeouts, query, {}, "", QueryProcessingStage::Complete, &settings, &query_client_info, false, {}, {});
                sent_query = true;
            }

            while (true)
            {
                std::lock_guard lock(mutex);
                if (was_cancelled)
                {
                    connection->disconnect();
                    finished = true;
                    return;
                }

                Packet packet = connection->receivePacket();
                switch (packet.type)
                {
                    case Protocol::Server::Data:
                        if (packet.block.rows() > 0)
                            got_data = true;
                        break;

                    case Protocol::Server::Exception:
                        finished = true;
                        packet.exception->rethrow();
                        break;

                    case Protocol::Server::EndOfStream:
                        finished = true;
                        return;

                    case Protocol::Server::Log:
                        if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                            log_queue->pushBlock(std::move(packet.block));
                        break;

                    case Protocol::Server::ProfileEvents:
                        if (auto profile_queue = CurrentThread::getInternalProfileEventsQueue())
                            if (!profile_queue->emplace(std::move(packet.block)))
                                throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push into profile queue");
                        break;

                    case Protocol::Server::Progress:
                    case Protocol::Server::ProfileInfo:
                    case Protocol::Server::Totals:
                    case Protocol::Server::Extremes:
                    case Protocol::Server::TimezoneUpdate:
                        break;

                    default:
                        connection->disconnect();
                        finished = true;
                        throw Exception(
                            ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}", packet.type, connection->getDescription());
                }
            }
        }

        void cancel()
        {
            std::lock_guard lock(mutex);
            if (was_cancelled || finished)
                return;

            was_cancelled = true;
            if (sent_query && connection->isConnected())
                connection->sendCancel();
        }

        bool isCancelled() const
        {
            std::lock_guard lock(mutex);
            return was_cancelled;
        }

        bool receivedData() const
        {
            std::lock_guard lock(mutex);
            return got_data;
        }

    private:
        IConnectionPool::Entry connection;
        const String query;
        const ContextPtr context;
        const ConnectionTimeouts timeouts;

        mutable Silk::FiberMutex mutex;
        bool sent_query TSA_GUARDED_BY(mutex) = false;
        bool finished TSA_GUARDED_BY(mutex) = false;
        bool was_cancelled TSA_GUARDED_BY(mutex) = false;
        bool got_data TSA_GUARDED_BY(mutex) = false;
    };

    /// Used to cancel the remote queries and unblock the dispatcher's workers on shutdown.
    /// Cancelling is required because otherwise server shutdown would be blocked on
    /// queries being executed on other clusters - arbitrarily long.
    class RemoteQueryExecutorRegistry
    {
    public:
        bool tryAdd(const std::shared_ptr<RemoteQueryExecutor> & executor)
        {
            std::lock_guard lock(mutex);
            if (finished)
                return false;
            executors.insert(executor);
            return true;
        }

        void remove(const std::shared_ptr<RemoteQueryExecutor> & executor)
        {
            std::lock_guard lock(mutex);
            executors.erase(executor);
        }

        void cancelAll()
        {
            std::unordered_set<std::shared_ptr<RemoteQueryExecutor>> snapshot;
            {
                std::lock_guard lock(mutex);
                finished = true;
                snapshot.swap(executors);
            }
            for (const auto & executor : snapshot)
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
        std::unordered_set<std::shared_ptr<RemoteQueryExecutor>> executors TSA_GUARDED_BY(mutex);
    };

    class RegisteredRemoteQueryExecutor
    {
    public:
        template <typename... Args>
        static std::optional<RegisteredRemoteQueryExecutor> tryCreate(RemoteQueryExecutorRegistry & registry, Args &&... args)
        {
            auto executor = std::make_shared<RemoteQueryExecutor>(std::forward<Args>(args)...);
            if (!registry.tryAdd(executor))
                return std::nullopt;
            return RegisteredRemoteQueryExecutor(registry, std::move(executor));
        }

        RegisteredRemoteQueryExecutor(RegisteredRemoteQueryExecutor &&) = default;

        ~RegisteredRemoteQueryExecutor()
        {
            if (executor)
                registry.remove(executor);
        }

        RemoteQueryExecutor * operator->() const { return executor.get(); }

    private:
        RegisteredRemoteQueryExecutor(RemoteQueryExecutorRegistry & registry_, std::shared_ptr<RemoteQueryExecutor> executor_)
            : registry(registry_), executor(std::move(executor_))
        {
        }

        RemoteQueryExecutorRegistry & registry;
        std::shared_ptr<RemoteQueryExecutor> executor;
    };

public:
    QueryRunnerDispatcher(
        ContextPtr global_context_,
        const String & cluster_name_,
        ShardSelector shard_selector_,
        LoggerPtr log_)
        : WithContext(global_context_)
        , cluster_name(cluster_name_)
        , shard_selector(shard_selector_)
        , log(log_)
    {
        client_info.client_name = String(client_name);
        client_info.setInitialQuery();
    }

    virtual ~QueryRunnerDispatcher() = default;

    virtual void submit(QueryRunnerJob job) = 0;

    PrefixLatch & getPending() { return pending; }

    void waitForAllPending(const QueryStatusPtr & query_status)
    {
        pending.waitForAllIssued(query_status);
    }

    void shutdown()
    {
        if (shutdown_called.exchange(true))
            return;

        cluster_executors.cancelAll();
        shutdownImpl();
    }

protected:
    void runJob(const QueryRunnerJob & job)
    {
        if (shutdown_called)
        {
            LOG_WARNING(log, "The table is shutting down, discarding the query");
            return;
        }

        try
        {
            auto job_context = makeJobContext(job);
            QueryScope query_scope = QueryScope::create(job_context);

            if (cluster_name.empty())
                executeLocally(job, job_context);
            else
                executeOnCluster(job, job_context);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to execute a query");
        }
    }

    virtual ConnectionPoolPtr createConnectionPool(const Cluster::Address & address, const String & database) = 0;

private:
    virtual void shutdownImpl() = 0;

    ContextMutablePtr makeJobContext(const QueryRunnerJob & job) const
    {
        auto job_context = Context::createCopy(getContext());
        job_context->makeQueryContext();
        job_context->setClientInfo(client_info);

        if (job.origin->user_id)
        {
            chassert(cluster_name.empty());
            /// Fail closed if the authentication method that queued this job has expired between the
            /// insert and now. The synchronous path re-checks per query in
            /// `Session::checkIfUserIsStillValid`; the deferred job has no session, so without this a
            /// token could enqueue work just before expiry and keep executing it afterwards.
            /// `authentication_valid_until` is 0 (no check) in the DEFINER/NONE cases and for an
            /// unrestricted credential.
            if (job.origin->authentication_valid_until != 0)
            {
                const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                if (now > job.origin->authentication_valid_until)
                    throw Exception(ErrorCodes::USER_EXPIRED, "Authentication method used to submit the deferred query has expired");
            }

            /// Replay the whole originating identity in one call: the external (pushed) roles, the
            /// credential grant limit and its expiry are restored together with the user, so a limited
            /// credential does not regain full rights and a pushed-role session does not fail role
            /// revalidation when the deferred job runs. `authentication_grants` is null in the
            /// DEFINER/NONE cases (a no-op).
            job_context->setUser(*job.origin->user_id, job.origin->external_roles, job.origin->authentication_grants, job.origin->authentication_valid_until);
        }
        if (job.origin->user_id && job.origin->roles)
        {
            chassert(cluster_name.empty());
            /// These are the session's *effective* current roles, which already include the external
            /// roles restored above. Re-apply them without the grant check: external roles are not
            /// locally granted, so a checked re-apply would throw `SET_NON_GRANTED_ROLE`; the locally
            /// granted current roles are kept and the external ones come from `setUser`.
            job_context->setCurrentRoles(*job.origin->roles, /*check_grants=*/ false);
        }

        job_context->setCurrentUserName(job.origin->current_user);
        job_context->setInitialUserName(job.origin->initial_user);
        job_context->setAuthenticatedUserName(job.origin->authenticated_user);

        job_context->setCurrentQueryId({});

        if (!job.settings_changes.empty())
        {
            /// In the cluster mode, the settings constraints are checked by the destination cluster.
            if (cluster_name.empty())
                job_context->checkSettingsConstraints(job.settings_changes, SettingSource::QUERY);
            job_context->applySettingsChanges(job.settings_changes);
        }

        /// After the job's settings, so the database explicitly recorded for the job wins over a
        /// `database` setting carried by the job's settings changes; `setCurrentDatabase` mirrors it
        /// back into the setting, keeping the two in sync for `executeQuery`.
        if (cluster_name.empty() && !job.database.empty())
            job_context->setCurrentDatabase(job.database);

        /// The engine always discards query results, so there is no point in transferring them over the network.
        job_context->setSetting("discard_query_data", true);

        return job_context;
    }

    void executeLocally(const QueryRunnerJob & job, ContextMutablePtr job_context) const
    {
        /// The job is nested, hence `internal` - which is also what marks these queries with
        /// `is_internal = 1` in `system.query_log`. Its text comes from the user who inserted it,
        /// hence `user_initiated`: without it the access checks of `CREATE` jobs would be skipped, so
        /// a job would not be limited to the privileges of the principal it runs as.
        auto io
            = executeQuery(job.query, job_context, QueryFlags{ .internal = true, .user_initiated = true }).second;
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
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The `QueryRunner` engine does not support this query");
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
            replica_pools.push_back(createConnectionPool(address, database));

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
        const auto query_start_time = std::chrono::system_clock::now();
        Stopwatch watch;
        std::optional<RegisteredRemoteQueryExecutor> executor;

        try
        {
            const auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(job.origin->settings);
            auto connection = getPool(shard_num, job.database)->getMany(timeouts, job.origin->settings, PoolMode::GET_ONE, {}, false).front();

            /// The remote server runs the job as an initial query and applies its binary type encoding settings to the wire,
            /// so this connection reads what the remote writes and writes what the remote reads.
            /// The remote checks the job's Avro schema registry URL against its own allowlist, not against this server's.
            Settings wire_settings = job_context->getSettingsCopy();
            wire_settings[Setting::format_avro_schema_registry_url] = "";
            auto format_settings = getFormatSettings(job_context, wire_settings);
            std::swap(format_settings.native.encode_types_in_binary_format, format_settings.native.decode_types_in_binary_format);
            connection->setFormatSettings(format_settings);

            auto registered = RegisteredRemoteQueryExecutor::tryCreate(cluster_executors, std::move(connection), job, job_context);
            if (!registered)
                return;
            executor.emplace(std::move(*registered));

            logClusterQuery(job, job_context, query_start_time, QueryLogElementType::QUERY_START, 0);
            (*executor)->execute();
        }
        catch (...)
        {
            logClusterQuery(job, job_context, query_start_time, QueryLogElementType::EXCEPTION_WHILE_PROCESSING, watch.elapsedMilliseconds());
            if (!shouldSkipShard(job.origin->settings, getCurrentExceptionCode(), executor && (*executor)->receivedData()))
                throw;

            LOG_WARNING(log, "Skipping shard {} due to `skip_unavailable_shards_mode` setting: {}", shard_num, getCurrentExceptionMessage(false));
            ProfileEvents::increment(ProfileEvents::DistributedShardsSkipped);
            return;
        }

        if (!(*executor)->isCancelled())
            logClusterQuery(job, job_context, query_start_time, QueryLogElementType::QUERY_FINISH, watch.elapsedMilliseconds());
    }

    void logClusterQuery(
        const QueryRunnerJob & job,
        ContextMutablePtr job_context,
        const std::chrono::system_clock::time_point query_start_time,
        const QueryLogElementType type,
        const UInt64 duration_ms) const
    {
        const auto & settings = job.origin->settings;
        if (!settings[Setting::log_queries] || type < settings[Setting::log_queries_min_type])
            return;

        const UInt64 min_duration_ms = settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds();
        if (duration_ms < min_duration_ms)
            return;

        auto query_log = getContext()->getQueryLog();
        if (!query_log)
            return;

        const auto event_time = std::chrono::system_clock::now();

        const String query_for_logging = formatQueryForLogging(job.query, settings);

        query_log->add([&](QueryLogElement & element)
        {
            element.type = type;
            element.event_time = timeInSeconds(event_time);
            element.event_time_microseconds = timeInMicroseconds(event_time);
            element.query_start_time = timeInSeconds(query_start_time);
            element.query_start_time_microseconds = timeInMicroseconds(query_start_time);
            element.query_duration_ms = duration_ms;
            element.query = query_for_logging;
            element.current_database = job.database;
            element.log_comment = job_context->getSettingsRef()[Setting::log_comment];
            element.client_info = job_context->getClientInfo();
            element.is_internal = true;

            if (settings[Setting::log_query_settings])
                element.query_settings = job_context->getSettingsRef().changedToFlatMap(/* show_secrets */ false);

            if (type == QueryLogElementType::EXCEPTION_WHILE_PROCESSING)
            {
                element.exception_code = getCurrentExceptionCode();
                element.exception = getCurrentExceptionMessage(false);
            }
        });
    }

    const String cluster_name;
    const ShardSelector shard_selector;
    ClientInfo client_info;
    PrefixLatch pending;

    std::mutex pools_mutex;
    std::map<std::pair<UInt64, String>, ConnectionPoolWithFailoverPtr> pools TSA_GUARDED_BY(pools_mutex);

    RemoteQueryExecutorRegistry cluster_executors;

protected:
    static constexpr std::string_view client_name = "QueryRunner";

    LoggerPtr log;

    std::atomic<bool> shutdown_called = false;
};

class QueryRunnerThreadPoolDispatcher : public QueryRunnerDispatcher
{
public:
    QueryRunnerThreadPoolDispatcher(
        ContextPtr global_context_,
        const String & cluster_name_,
        ShardSelector shard_selector_,
        UInt64 num_threads_,
        UInt64 max_pending_queries_,
        UInt64 connection_pool_size_,
        LoggerPtr log_)
        : QueryRunnerDispatcher(global_context_, cluster_name_, shard_selector_, log_)
        , max_pending_queries(max_pending_queries_)
        , connection_pool_size(connection_pool_size_)
        , pool(
              CurrentMetrics::QueryRunnerThreads,
              CurrentMetrics::QueryRunnerThreadsActive,
              CurrentMetrics::QueryRunnerThreadsScheduled,
              max_pending_queries_ ? std::min(num_threads_, max_pending_queries_) : num_threads_,
              0,
              max_pending_queries_)
    {
    }

    void submit(QueryRunnerJob job) override
    {
        if (pool.trySchedule([this, scheduled_job = std::move(job)]
            {
                setThreadName(ThreadName::QUERY_RUNNER);
                runJob(scheduled_job);
            }))
            return;

        if (shutdown_called)
            LOG_WARNING(log, "The table is shutting down, discarding the query");
        else
            LOG_ERROR(LogFrequencyLimiter(log, 5), "Too many pending queries (max_pending_queries = {}), discarding the query", max_pending_queries);
    }

private:
    void shutdownImpl() override
    {
        pool.finish();
        pool.wait();
    }

    ConnectionPoolPtr createConnectionPool(const Cluster::Address & address, const String & database) override
    {
        return ConnectionPoolFactory::instance().get(
            static_cast<unsigned>(connection_pool_size),
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
            address.priority);
    }

    const size_t max_pending_queries;
    const size_t connection_pool_size;
    ThreadPool pool;
};

#if USE_SILK

class QueryRunnerFiberDispatcher : public QueryRunnerDispatcher
{
public:
    QueryRunnerFiberDispatcher(
        ContextPtr global_context_,
        const String & cluster_name_,
        ShardSelector shard_selector_,
        UInt64 max_pending_queries_,
        UInt64 connection_pool_size_,
        LoggerPtr log_)
        : QueryRunnerDispatcher(global_context_, cluster_name_, shard_selector_, log_)
        , max_pending_queries(max_pending_queries_)
        , connection_pool_size(connection_pool_size_)
    {
    }

    void submit(QueryRunnerJob job) override
    {
        std::lock_guard lock(futures_mutex);

        if (fibers_finished)
        {
            LOG_WARNING(log, "The table is shutting down, discarding the query");
            return;
        }

        /// Reap finished fibers.
        for (auto it = futures.begin(); it != futures.end();)
        {
            int error = 0;
            if (it->isSet(&error))
            {
                it = futures.erase(it);
                if (error)
                    LOG_ERROR(LogFrequencyLimiter(log, 5), "QueryRunner fiber finished with error code {}.", error);
            }
            else
            {
                ++it;
            }
        }

        if (max_pending_queries && futures.size() >= max_pending_queries)
        {
            LOG_ERROR(LogFrequencyLimiter(log, 5), "Too many pending queries (max_pending_queries = {}), discarding the query", max_pending_queries);
            return;
        }

        auto & future = futures.emplace_back();
        try
        {
            const int error = Silk::spawn(
                [this, fiber_job = std::move(job)]() -> int
                {
                    runJob(fiber_job);
                    return 0;
                },
                future);
            if (error)
                throw Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot spawn a fiber to execute the query (error code: {})", error);
        }
        catch (...)
        {
            futures.pop_back();
            throw;
        }
    }

private:
    void shutdownImpl() override
    {
        std::lock_guard lock(futures_mutex);
        fibers_finished = true;
        for (auto & future : futures)
            future.wait();
        futures.clear();
    }

    ConnectionPoolPtr createConnectionPool(const Cluster::Address & address, const String & database) override
    {
        return std::make_shared<Silk::ConnectionPool>(
            static_cast<unsigned>(connection_pool_size),
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
            address.priority,
            Silk::streamSocketFactory());
    }

    const UInt64 max_pending_queries;
    const UInt64 connection_pool_size;

    std::mutex futures_mutex;
    std::list<silk::FiberFuture> futures TSA_GUARDED_BY(futures_mutex);
    bool fibers_finished TSA_GUARDED_BY(futures_mutex) = false;
};

#endif


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
            job.completion = std::make_shared<QueryRunnerJobCompletion>(dispatcher.getPending(), batch);

            dispatcher.submit(std::move(job));
        }

        if (batch)
        {
            while (!batch->wait(poll_interval))
            {
                if (query_status)
                {
                    query_status->checkTimeLimit();
                }
            }
        }
    }

private:
    static constexpr std::chrono::milliseconds poll_interval{100};

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
    }
    else if (cluster_name.empty())
        storage_metadata.sql_security_type = SQLSecurityType::INVOKER;

    setInMemoryMetadata(storage_metadata);

    if (settings[QueryRunnerSetting::scheduler] == WorkerKind::FIBERS)
    {
        if (cluster_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'fibers' scheduler of the QueryRunner engine requires the 'cluster' setting");
#if USE_SILK
        if (!Silk::isFiberSchedulerInitialized())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "The 'fibers' scheduler of the QueryRunner engine requires the 'enable_silk_runtime' server setting");

        dispatcher = std::make_unique<QueryRunnerFiberDispatcher>(
            getContext(),
            cluster_name,
            parseShardSelector(settings[QueryRunnerSetting::shard]),
            settings[QueryRunnerSetting::max_pending_queries],
            settings[QueryRunnerSetting::connection_pool_size],
            log);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The 'fibers' scheduler of the QueryRunner engine requires a build with silk");
#endif
    }
    else
        dispatcher = std::make_unique<QueryRunnerThreadPoolDispatcher>(
            getContext(),
            cluster_name,
            parseShardSelector(settings[QueryRunnerSetting::shard]),
            settings[QueryRunnerSetting::threads],
            settings[QueryRunnerSetting::max_pending_queries],
            settings[QueryRunnerSetting::connection_pool_size],
            log);

    if (storage_metadata.sql_security_type == SQLSecurityType::DEFINER)
        DefinerDependencies::instance().addDependency(*storage_metadata.definer, table_id_);
}

StorageQueryRunner::~StorageQueryRunner() = default;

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
            .external_roles = {},
            .authentication_grants = {},
            .authentication_valid_until = 0,
            .current_user = {},
            .initial_user = {},
            .authenticated_user = inserter.authenticated_user,
            .settings = local_context->getSettingsCopy(),
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
                    .external_roles = local_context->getExternalRoles(),
                    .authentication_grants = local_context->getAuthenticationGrants(),
                    .authentication_valid_until = local_context->getAuthenticationValidUntil(),
                    .current_user = inserter.current_user,
                    .initial_user = inserter.initial_user,
                    .authenticated_user = inserter.authenticated_user,
                    .settings = local_context->getSettingsCopy(),
                });
                break;
            case SQLSecurityType::DEFINER:
                origin = std::make_shared<const QueryRunnerJobOrigin>(QueryRunnerJobOrigin{
                    .user_id = metadata_snapshot->getDefinerID(local_context),
                    .roles = {},
                    .external_roles = {},
                    .authentication_grants = {},
                    .authentication_valid_until = 0,
                    .current_user = *metadata_snapshot->definer,
                    .initial_user = *metadata_snapshot->definer,
                    .authenticated_user = inserter.authenticated_user,
                    .settings = local_context->getSettingsCopy(),
                });
                break;
            case SQLSecurityType::NONE:
                origin = std::make_shared<const QueryRunnerJobOrigin>(QueryRunnerJobOrigin{
                    .user_id = {},
                    .roles = {},
                    .external_roles = {},
                    .authentication_grants = {},
                    .authentication_valid_until = 0,
                    .current_user = {},
                    .initial_user = {},
                    .authenticated_user = inserter.authenticated_user,
                    .settings = local_context->getSettingsCopy(),
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
        if (num_threads > 1024)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'threads' setting of the QueryRunner engine must be in the range [1, 1024], got {}", num_threads);

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

        if (settings[QueryRunnerSetting::connection_pool_size] < 1 || settings[QueryRunnerSetting::connection_pool_size] > std::numeric_limits<unsigned>::max())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The 'connection_pool_size' setting of the QueryRunner engine must be in the range [1, {}], got {}",
                std::numeric_limits<unsigned>::max(), settings[QueryRunnerSetting::connection_pool_size].value);

        if (settings[QueryRunnerSetting::scheduler] == WorkerKind::FIBERS && settings[QueryRunnerSetting::threads].changed)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'threads' setting of the QueryRunner engine does not apply to the 'fibers' scheduler");

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
    SecretArgumentsSpec{},
    {
        .supports_settings = true,
        .supports_parallel_insert = true,
        .supports_sql_security = true,
        .has_builtin_setting_fn = QueryRunnerSettings::hasBuiltin,
    },
    Documentation{.description = R"DOC(A table engine whose write path runs queries instead of storing data. Rows inserted into a `QueryRunner` table are dispatched as queries to be executed (synchronously or asynchronously); it is used to orchestrate and run queries through an `INSERT` interface.)DOC"});
}

}
