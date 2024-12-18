#include <Interpreters/ProcessList.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/queryNormalization.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <chrono>


namespace CurrentMetrics
{
    extern const Metric Query;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_concurrent_queries_for_all_users;
    extern const SettingsUInt64 max_concurrent_queries_for_user;
    extern const SettingsSeconds max_execution_time;
    extern const SettingsUInt64 max_memory_usage;
    extern const SettingsUInt64 max_memory_usage_for_user;
    extern const SettingsUInt64 max_network_bandwidth_for_all_users;
    extern const SettingsUInt64 max_network_bandwidth_for_user;
    extern const SettingsUInt64 max_temporary_data_on_disk_size_for_user;
    extern const SettingsUInt64 memory_usage_overcommit_max_wait_microseconds;
    extern const SettingsUInt64 memory_overcommit_ratio_denominator;
    extern const SettingsUInt64 memory_overcommit_ratio_denominator_for_user;
    extern const SettingsUInt64 memory_profiler_step;
    extern const SettingsUInt64 memory_profiler_sample_min_allocation_size;
    extern const SettingsUInt64 memory_profiler_sample_max_allocation_size;
    extern const SettingsFloat memory_profiler_sample_probability;
    extern const SettingsUInt64 max_temporary_data_on_disk_size_for_query;
    extern const SettingsFloat memory_tracker_fault_probability;
    extern const SettingsUInt64 priority;
    extern const SettingsMilliseconds queue_max_wait_ms;
    extern const SettingsBool replace_running_query;
    extern const SettingsMilliseconds replace_running_query_max_wait_ms;
    extern const SettingsString temporary_files_codec;
    extern const SettingsOverflowMode timeout_overflow_mode;
    extern const SettingsBool trace_profile_events;
}

namespace ErrorCodes
{
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}


/// Should we execute the query even if max_concurrent_queries limit is exhausted
static bool isUnlimitedQuery(const IAST * ast)
{
    if (!ast)
        return false;

    /// It is KILL QUERY or an async insert flush query
    if (ast->as<ASTKillQueryQuery>() || ast->getQueryKind() == IAST::QueryKind::AsyncInsertFlush)
        return true;

    /// It is SELECT FROM system.processes
    /// NOTE: This is very rough check.
    /// False negative: USE system; SELECT * FROM processes;
    /// False positive: SELECT * FROM system.processes CROSS JOIN (SELECT ...)

    if (const auto * ast_selects = ast->as<ASTSelectWithUnionQuery>())
    {
        if (!ast_selects->list_of_selects || ast_selects->list_of_selects->children.empty())
            return false;

        const auto * ast_select = ast_selects->list_of_selects->children[0]->as<ASTSelectQuery>();
        if (!ast_select)
            return false;

        if (auto database_and_table = getDatabaseAndTable(*ast_select, 0))
            return database_and_table->database == "system" && database_and_table->table == "processes";

        return false;
    }

    return false;
}


ProcessList::EntryPtr
ProcessList::insert(const String & query_, const IAST * ast, ContextMutablePtr query_context, UInt64 watch_start_nanoseconds)
{
    EntryPtr res;

    const ClientInfo & client_info = query_context->getClientInfo();
    const Settings & settings = query_context->getSettingsRef();

    if (client_info.current_query_id.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query id cannot be empty");

    bool is_unlimited_query = isUnlimitedQuery(ast);

    {
        auto [lock, overcommit_blocker] = safeLock(); // To avoid deadlock in case of OOM
        IAST::QueryKind query_kind = ast->getQueryKind();

        const auto queue_max_wait_ms = settings[Setting::queue_max_wait_ms].totalMilliseconds();
        UInt64 waiting_queries = waiting_queries_amount.load();
        if (!is_unlimited_query && max_size && processes.size() >= max_size + waiting_queries)
        {
            if (queue_max_wait_ms)
                LOG_WARNING(getLogger("ProcessList"), "Too many simultaneous queries, will wait {} ms.", queue_max_wait_ms);
            if (!queue_max_wait_ms || !have_space.wait_for(lock, std::chrono::milliseconds(queue_max_wait_ms),
                    [&]{ waiting_queries = waiting_queries_amount.load(); return processes.size() < max_size + waiting_queries; }))
                throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                "Too many simultaneous queries. Maximum: {}{}",
                                max_size, waiting_queries == 0 ? "" : fmt::format(", waiting: {}", waiting_queries));
        }

        if (!is_unlimited_query)
        {
            QueryAmount amount = getQueryKindAmount(query_kind);
            UInt64 waiting_inserts = waiting_insert_queries_amount.load();
            UInt64 waiting_selects = waiting_select_queries_amount.load();
            if (max_insert_queries_amount && query_kind == IAST::QueryKind::Insert && amount >= max_insert_queries_amount + waiting_inserts)
                throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                "Too many simultaneous insert queries. Maximum: {}, current: {}{}",
                                max_insert_queries_amount, amount, waiting_inserts == 0 ? "" : fmt::format(", waiting: {}", waiting_inserts));
            if (max_select_queries_amount && query_kind == IAST::QueryKind::Select && amount >= max_select_queries_amount + waiting_selects)
                throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                "Too many simultaneous select queries. Maximum: {}, current: {}{}",
                                max_select_queries_amount, amount, waiting_selects == 0 ? "" : fmt::format(", waiting: {}", waiting_selects));
        }

        {
            /**
             * `max_size` check above is controlled by `max_concurrent_queries` server setting and is a "hard" limit for how many
             * queries the server can process concurrently. It is configured at startup. When the server is overloaded with queries and the
             * hard limit is reached it is impossible to connect to the server to run queries for investigation.
             *
             * With `max_concurrent_queries_for_all_users` it is possible to configure an additional, runtime configurable, limit for query concurrency.
             * Usually it should be configured just once for `default_profile` which is inherited by all users. DBAs can override
             * this setting when connecting to ClickHouse, or it can be configured for a DBA profile to have a value greater than that of
             * the default profile (or 0 for unlimited).
             *
             * One example is to set `max_size=X`, `max_concurrent_queries_for_all_users=X-10` for default profile,
             * and `max_concurrent_queries_for_all_users=0` for DBAs or accounts that are vital for ClickHouse operations (like metrics
             * exporters).
             *
             * Another creative example is to configure `max_concurrent_queries_for_all_users=50` for "analyst" profiles running adhoc queries
             * and `max_concurrent_queries_for_all_users=100` for "customer facing" services. This way "analyst" queries will be rejected
             * once is already processing 50+ concurrent queries (including analysts or any other users).
             */

            waiting_queries = waiting_queries_amount.load();
            if (!is_unlimited_query && settings[Setting::max_concurrent_queries_for_all_users]
                && processes.size() >= settings[Setting::max_concurrent_queries_for_all_users] + waiting_queries_amount)
                throw Exception(
                    ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                    "Too many simultaneous queries for all users. "
                    "Current: {}, maximum: {}{}",
                    processes.size(),
                    settings[Setting::max_concurrent_queries_for_all_users].toString(),
                    waiting_queries == 0 ? "" : fmt::format(", waiting: {}", waiting_queries));
        }

        /** Why we use current user?
          * Because initial one is passed by client and credentials for it is not verified,
          *  and using initial_user for limits will be insecure.
          *
          * Why we use current_query_id?
          * Because we want to allow distributed queries that will run multiple secondary queries on same server,
          *  like SELECT count() FROM remote('127.0.0.{1,2}', system.numbers)
          *  so they must have different query_ids.
          */

        {
            auto user_process_list = user_to_queries.find(client_info.current_user);

            if (user_process_list != user_to_queries.end())
            {
                UInt64 user_waiting_queries = user_process_list->second.waiting_queries_amount.load();
                if (!is_unlimited_query && settings[Setting::max_concurrent_queries_for_user]
                    && user_process_list->second.queries.size() >= settings[Setting::max_concurrent_queries_for_user] + user_waiting_queries)
                    throw Exception(
                        ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                        "Too many simultaneous queries for user {}. "
                        "Current: {}, maximum: {}{}",
                        client_info.current_user,
                        user_process_list->second.queries.size(),
                        settings[Setting::max_concurrent_queries_for_user].toString(),
                        user_waiting_queries == 0 ? "" : fmt::format(", waiting: {}", user_waiting_queries));

                auto running_query = user_process_list->second.queries.find(client_info.current_query_id);

                if (running_query != user_process_list->second.queries.end())
                {
                    if (!settings[Setting::replace_running_query])
                        throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING, "Query with id = {} is already running.", client_info.current_query_id);

                    /// Ask queries to cancel. They will check this flag.
                    running_query->second->is_killed.store(true, std::memory_order_relaxed);

                    const auto replace_running_query_max_wait_ms = settings[Setting::replace_running_query_max_wait_ms].totalMilliseconds();
                    if (!replace_running_query_max_wait_ms || !have_space.wait_for(lock, std::chrono::milliseconds(replace_running_query_max_wait_ms),
                        [&]
                        {
                            running_query = user_process_list->second.queries.find(client_info.current_query_id);
                            if (running_query == user_process_list->second.queries.end())
                                return true;
                            running_query->second->is_killed.store(true, std::memory_order_relaxed);
                            return false;
                        }))
                    {
                        throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING,
                                        "Query with id = {} is already running and can't be stopped",
                                        client_info.current_query_id);
                    }
                }
            }
        }

        /// Check other users running query with our query_id
        if (auto query_user = queries_to_user.find(client_info.current_query_id); query_user != queries_to_user.end() && query_user->second != client_info.current_user)
        {
            throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING,
                            "Query with id = {} is already running by user {}",
                            client_info.current_query_id, query_user->second);
        }

        auto user_process_list_it = user_to_queries.find(client_info.current_user);
        if (user_process_list_it == user_to_queries.end())
        {
            user_process_list_it = user_to_queries.emplace(std::piecewise_construct,
                std::forward_as_tuple(client_info.current_user),
                std::forward_as_tuple(query_context->getGlobalContext(), this)).first;
        }
        ProcessListForUser & user_process_list = user_process_list_it->second;

        /// Actualize thread group info
        CurrentThread::attachQueryForLog(query_);
        auto thread_group = CurrentThread::getGroup();
        if (thread_group)
        {
            thread_group->performance_counters.setParent(&user_process_list.user_performance_counters);
            thread_group->memory_tracker.setParent(&user_process_list.user_memory_tracker);
            if (user_process_list.user_temp_data_on_disk)
            {
                TemporaryDataOnDiskSettings temporary_data_on_disk_settings
                {
                    .max_size_on_disk = settings[Setting::max_temporary_data_on_disk_size_for_query],
                    .compression_codec = settings[Setting::temporary_files_codec]
                };
                query_context->setTempDataOnDisk(std::make_shared<TemporaryDataOnDiskScope>(
                    user_process_list.user_temp_data_on_disk, std::move(temporary_data_on_disk_settings)));
            }

            /// Set query-level memory trackers
            thread_group->memory_tracker.setOrRaiseHardLimit(settings[Setting::max_memory_usage]);
            thread_group->memory_tracker.setSoftLimit(settings[Setting::memory_overcommit_ratio_denominator]);

            if (query_context->hasTraceCollector())
            {
                /// Set up memory profiling
                thread_group->memory_tracker.setProfilerStep(settings[Setting::memory_profiler_step]);

                thread_group->memory_tracker.setSampleProbability(settings[Setting::memory_profiler_sample_probability]);
                thread_group->memory_tracker.setSampleMinAllocationSize(settings[Setting::memory_profiler_sample_min_allocation_size]);
                thread_group->memory_tracker.setSampleMaxAllocationSize(settings[Setting::memory_profiler_sample_max_allocation_size]);
                thread_group->performance_counters.setTraceProfileEvents(settings[Setting::trace_profile_events]);
            }

            thread_group->memory_tracker.setDescription("(for query)");
            if (settings[Setting::memory_tracker_fault_probability] > 0.0)
                thread_group->memory_tracker.setFaultProbability(settings[Setting::memory_tracker_fault_probability]);

            thread_group->memory_tracker.setOvercommitWaitingTime(settings[Setting::memory_usage_overcommit_max_wait_microseconds]);

            /// NOTE: Do not set the limit for thread-level memory tracker since it could show unreal values
            ///  since allocation and deallocation could happen in different threads
        }

        auto process_it = processes.emplace(
            processes.end(),
            std::make_shared<QueryStatus>(
                query_context,
                query_,
                client_info,
                priorities.insert(settings[Setting::priority]),
                std::move(thread_group),
                query_kind,
                settings,
                watch_start_nanoseconds));

        increaseQueryKindAmount(query_kind);

        res = std::make_shared<Entry>(*this, process_it);

        (*process_it)->setUserProcessList(&user_process_list);
        (*process_it)->setProcessListEntry(res);

        user_process_list.queries.emplace(client_info.current_query_id, res->getQueryStatus());
        queries_to_user.emplace(client_info.current_query_id, client_info.current_user);

        /// Track memory usage for all simultaneously running queries from single user.
        user_process_list.user_memory_tracker.setOrRaiseHardLimit(settings[Setting::max_memory_usage_for_user]);
        user_process_list.user_memory_tracker.setSoftLimit(settings[Setting::memory_overcommit_ratio_denominator_for_user]);
        user_process_list.user_memory_tracker.setDescription("(for user)");

        if (!total_network_throttler && settings[Setting::max_network_bandwidth_for_all_users])
        {
            total_network_throttler = std::make_shared<Throttler>(settings[Setting::max_network_bandwidth_for_all_users]);
        }

        if (!user_process_list.user_throttler)
        {
            if (settings[Setting::max_network_bandwidth_for_user])
                user_process_list.user_throttler
                    = std::make_shared<Throttler>(settings[Setting::max_network_bandwidth_for_user], total_network_throttler);
            else if (settings[Setting::max_network_bandwidth_for_all_users])
                user_process_list.user_throttler = total_network_throttler;
        }
    }

    return res;
}


ProcessListEntry::~ProcessListEntry()
{
    auto lock = parent.safeLock();

    String user = (*it)->getClientInfo().current_user;
    String query_id = (*it)->getClientInfo().current_query_id;
    IAST::QueryKind query_kind = (*it)->query_kind;

    const QueryStatusPtr process_list_element_ptr = *it;

    auto user_process_list_it = parent.user_to_queries.find(user);
    if (user_process_list_it == parent.user_to_queries.end())
    {
        LOG_ERROR(getLogger("ProcessList"), "Cannot find user in ProcessList");
        std::terminate();
    }

    ProcessListForUser & user_process_list = user_process_list_it->second;

    bool found = false;

    if (auto running_query = user_process_list.queries.find(query_id); running_query != user_process_list.queries.end())
    {
        if (running_query->second == process_list_element_ptr)
        {
            user_process_list.queries.erase(running_query->first);
            found = true;
        }
    }

    /// Wait for the query if it is in the cancellation right now.
    parent.cancelled_cv.wait(lock.lock, [&]() { return process_list_element_ptr->is_cancelling == false; });

    if (auto query_user = parent.queries_to_user.find(query_id); query_user != parent.queries_to_user.end())
        parent.queries_to_user.erase(query_user);

    /// This removes the memory_tracker of one request.
    parent.processes.erase(it);

    if (!found)
    {
        LOG_ERROR(getLogger("ProcessList"), "Cannot find query by query_id and pointer to ProcessListElement in ProcessListForUser");
        std::terminate();
    }

    parent.decreaseQueryKindAmount(query_kind);

    parent.have_space.notify_all();

    /// If there are no more queries for the user, then we will reset memory tracker and network throttler.
    if (user_process_list.queries.empty())
        user_process_list.resetTrackers();

    /// Reset throttler, similarly (see above).
    if (parent.processes.empty())
        parent.total_network_throttler.reset();
}


QueryStatus::QueryStatus(
    ContextPtr context_,
    const String & query_,
    const ClientInfo & client_info_,
    QueryPriorities::Handle && priority_handle_,
    ThreadGroupPtr && thread_group_,
    IAST::QueryKind query_kind_,
    const Settings & query_settings_,
    UInt64 watch_start_nanoseconds)
    : WithContext(context_)
    , query(query_)
    , client_info(client_info_)
    , thread_group(std::move(thread_group_))
    , watch(CLOCK_MONOTONIC, watch_start_nanoseconds, true)
    , priority_handle(std::move(priority_handle_))
    , global_overcommit_tracker(context_->getGlobalOvercommitTracker())
    , query_kind(query_kind_)
    , num_queries_increment(CurrentMetrics::Query)
{
    /// We have to pass `query_settings_` to this constructor because we can't use `context_->getSettings().max_execution_time` here:
    /// a QueryStatus is created with `ProcessList::mutex` locked (see ProcessList::insert) and calling `context_->getSettings()`
    /// would lock the context's lock too, whereas holding two those locks simultaneously is not good.
    limits.max_execution_time = query_settings_[Setting::max_execution_time];
    overflow_mode = query_settings_[Setting::timeout_overflow_mode];
}

QueryStatus::~QueryStatus()
{
#if !defined(NDEBUG)
    /// Check that all executors were invalidated.
    for (const auto & [_, e] : executors)
        assert(!e->executor);
#endif

    if (auto * memory_tracker = getMemoryTracker())
    {
        if (user_process_list)
            user_process_list->user_overcommit_tracker.onQueryStop(memory_tracker);
        if (global_overcommit_tracker)
            global_overcommit_tracker->onQueryStop(memory_tracker);
    }
}

void QueryStatus::ExecutorHolder::cancel()
{
    std::lock_guard lock(mutex);
    if (executor)
        executor->cancel();
}

void QueryStatus::ExecutorHolder::remove()
{
    std::lock_guard lock(mutex);
    executor = nullptr;
}

CancellationCode QueryStatus::cancelQuery(bool)
{
    if (is_killed.load())
        return CancellationCode::CancelSent;

    is_killed.store(true);

    std::vector<ExecutorHolderPtr> executors_snapshot;

    {
        /// Create a snapshot of executors under a mutex.
        std::lock_guard lock(executors_mutex);
        executors_snapshot.reserve(executors.size());
        for (const auto & [_, e] : executors)
            executors_snapshot.push_back(e);
    }

    /// We should call cancel() for each executor with unlocked executors_mutex, because
    /// cancel() can try to lock some internal mutex that is already locked by query executing
    /// thread, and query executing thread can call removePipelineExecutor and lock executors_mutex,
    /// which will lead to deadlock.
    /// Note that the size and the content of executors cannot be changed while
    /// executors_mutex is unlocked, because:
    /// 1) We don't allow adding new executors while cancelling query in addPipelineExecutor
    /// 2) We don't actually remove executor holder from executors in removePipelineExecutor,
    /// just mark that executor is invalid.
    /// So, it's ok to use a snapshot created above under a mutex, it won't be any differ from actual executors.
    for (const auto & e : executors_snapshot)
        e->cancel();

    return CancellationCode::CancelSent;
}

void QueryStatus::addPipelineExecutor(PipelineExecutor * e)
{
    /// In case of asynchronous distributed queries it is possible to call
    /// addPipelineExecutor() from the cancelQuery() context, and this will
    /// lead to deadlock.
    if (is_killed.load())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

    std::lock_guard lock(executors_mutex);
    assert(!executors.contains(e));
    executors[e] = std::make_shared<ExecutorHolder>(e);
}

void QueryStatus::removePipelineExecutor(PipelineExecutor * e)
{
    ExecutorHolderPtr executor_holder;

    {
        std::lock_guard lock(executors_mutex);
        assert(executors.contains(e));
        executor_holder = executors[e];
        executors.erase(e);
    }

    /// Invalidate executor pointer inside holder.
    /// We should do it with released executors_mutex to avoid possible lock order inversion.
    executor_holder->remove();
}

bool QueryStatus::checkTimeLimit()
{
    if (is_killed.load())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

    return limits.checkTimeLimit(watch, overflow_mode);
}

bool QueryStatus::checkTimeLimitSoft()
{
    if (is_killed.load())
        return false;

    return limits.checkTimeLimit(watch, OverflowMode::BREAK);
}


void QueryStatus::setUserProcessList(ProcessListForUser * user_process_list_)
{
    user_process_list = user_process_list_;
}


void QueryStatus::setProcessListEntry(std::weak_ptr<ProcessListEntry> process_list_entry_)
{
    /// Synchronization is not required here because this function is only called from ProcessList::insert()
    /// when `ProcessList::mutex` is locked.
    if (!process_list_entry.expired() && !process_list_entry_.expired())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Two entries in the process list cannot both use the same query status");
    process_list_entry = process_list_entry_;
}


std::shared_ptr<ProcessListEntry> QueryStatus::getProcessListEntry() const
{
    return process_list_entry.lock();
}


ThrottlerPtr QueryStatus::getUserNetworkThrottler()
{
    if (!user_process_list)
        return {};
    return user_process_list->user_throttler;
}


QueryStatusPtr ProcessList::tryGetProcessListElement(const String & current_query_id, const String & current_user)
{
    auto user_it = user_to_queries.find(current_user);
    if (user_it != user_to_queries.end())
    {
        const auto & user_queries = user_it->second.queries;
        auto query_it = user_queries.find(current_query_id);

        if (query_it != user_queries.end())
            return query_it->second;
    }

    return {};
}


CancellationCode ProcessList::sendCancelToQuery(const String & current_query_id, const String & current_user, bool kill)
{
    QueryStatusPtr elem;

    /// Cancelling the query should be done without the lock.
    ///
    /// Since it may be not that trivial, for example in case of distributed
    /// queries it tries to cancel the query gracefully on shards and this can
    /// take a while, so acquiring a lock during this time will lead to wait
    /// all new queries for this cancellation.
    ///
    /// Another problem is that it can lead to a deadlock, because of
    /// OvercommitTracker.
    ///
    /// So here we first set is_cancelling, and later reset it.
    /// The ProcessListEntry cannot be destroy if is_cancelling is true.
    {
        auto lock = safeLock();
        elem = tryGetProcessListElement(current_query_id, current_user);
        if (!elem)
            return CancellationCode::NotFound;
        elem->is_cancelling = true;
    }

    SCOPE_EXIT({
        DENY_ALLOCATIONS_IN_SCOPE;

        auto lock = unsafeLock();
        elem->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    return elem->cancelQuery(kill);
}


CancellationCode ProcessList::sendCancelToQuery(QueryStatusPtr elem, bool kill)
{
    /// Cancelling the query should be done without the lock.
    /// So here we first set is_cancelling, and later reset it.
    /// The ProcessListEntry cannot be destroy if is_cancelling is true.
    {
        auto lock = safeLock();
        elem->is_cancelling = true;
    }

    SCOPE_EXIT({
        DENY_ALLOCATIONS_IN_SCOPE;

        auto lock = unsafeLock();
        elem->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    return elem->cancelQuery(kill);
}


void ProcessList::killAllQueries()
{
    std::vector<QueryStatusPtr> cancelled_processes;

    SCOPE_EXIT({
        auto lock = safeLock();
        for (auto & cancelled_process : cancelled_processes)
            cancelled_process->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    {
        auto lock = safeLock();
        cancelled_processes.reserve(processes.size());
        for (auto & process : processes)
        {
            cancelled_processes.push_back(process);
            process->is_cancelling = true;
        }
    }

    for (auto & cancelled_process : cancelled_processes)
        cancelled_process->cancelQuery(true);

}


QueryStatusInfo QueryStatus::getInfo(bool get_thread_list, bool get_profile_events, bool get_settings) const
{
    QueryStatusInfo res{};

    res.query             = query;
    res.query_kind        = query_kind;
    res.client_info       = client_info;
    res.elapsed_microseconds = watch.elapsedMicroseconds();
    res.is_cancelled      = is_killed.load(std::memory_order_relaxed);
    res.is_all_data_sent  = is_all_data_sent.load(std::memory_order_relaxed);
    res.read_rows         = progress_in.read_rows;
    res.read_bytes        = progress_in.read_bytes;
    res.total_rows        = progress_in.total_rows_to_read;

    res.written_rows      = progress_out.written_rows;
    res.written_bytes     = progress_out.written_bytes;

    if (thread_group)
    {
        res.memory_usage = thread_group->memory_tracker.get();
        res.peak_memory_usage = thread_group->memory_tracker.getPeak();

        if (get_thread_list)
        {
            res.thread_ids = thread_group->getInvolvedThreadIds();
            res.peak_threads_usage = thread_group->getPeakThreadsUsage();
        }
        if (get_profile_events)
            res.profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(thread_group->performance_counters.getPartiallyAtomicSnapshot());
    }

    if (get_settings)
    {
        if (auto ctx = context.lock())
        {
            res.query_settings = std::make_shared<Settings>(ctx->getSettingsCopy());
            res.current_database = ctx->getCurrentDatabase();
        }
    }

    return res;
}


ProcessList::Info ProcessList::getInfo(bool get_thread_list, bool get_profile_events, bool get_settings) const
{
    /// We have to copy `processes` first because `process->getInfo()` below can access the context to get the query settings,
    /// and it's better not to keep the process list's lock while doing that.
    std::vector<QueryStatusPtr> processes_copy;

    {
        auto lock = safeLock();
        processes_copy.assign(processes.begin(), processes.end());
    }

    Info per_query_infos;
    per_query_infos.reserve(processes_copy.size());
    for (const auto & process : processes_copy)
        per_query_infos.emplace_back(process->getInfo(get_thread_list, get_profile_events, get_settings));

    return per_query_infos;
}


ProcessListForUser::ProcessListForUser(ProcessList * global_process_list)
    : ProcessListForUser(nullptr, global_process_list)
{}

ProcessListForUser::ProcessListForUser(ContextPtr global_context, ProcessList * global_process_list)
    : user_overcommit_tracker(global_process_list, this)
{
    user_memory_tracker.setOvercommitTracker(&user_overcommit_tracker);

    if (global_context)
    {
        const auto & settings = global_context->getSettingsRef();
        TemporaryDataOnDiskSettings temporary_data_on_disk_settings
        {
            .max_size_on_disk = settings[Setting::max_temporary_data_on_disk_size_for_user],
            .compression_codec = settings[Setting::temporary_files_codec]
        };

        user_temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(global_context->getSharedTempDataOnDisk(),
            std::move(temporary_data_on_disk_settings));
    }
}


ProcessListForUserInfo ProcessListForUser::getInfo(bool get_profile_events) const
{
    ProcessListForUserInfo res;

    res.memory_usage = user_memory_tracker.get();
    res.peak_memory_usage = user_memory_tracker.getPeak();

    if (get_profile_events)
        res.profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(user_performance_counters.getPartiallyAtomicSnapshot());

    return res;
}


ProcessList::UserInfo ProcessList::getUserInfo(bool get_profile_events) const
{
    UserInfo per_user_infos;

    auto lock = safeLock();

    per_user_infos.reserve(user_to_queries.size());

    for (const auto & [user, user_queries] : user_to_queries)
        per_user_infos.emplace(user, user_queries.getInfo(get_profile_events));

    return per_user_infos;
}

void ProcessList::increaseQueryKindAmount(const IAST::QueryKind & query_kind)
{
    auto found = query_kind_amounts.find(query_kind);
    if (found == query_kind_amounts.end())
        query_kind_amounts[query_kind] = 1;
    else
        found->second += 1;
}

void ProcessList::decreaseQueryKindAmount(const IAST::QueryKind & query_kind)
{
    auto found = query_kind_amounts.find(query_kind);
    /// TODO: we could just rebuild the map, as we have saved all query_kind.
    if (found == query_kind_amounts.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query kind amount: decrease before increase on '{}'", query_kind);
    else if (found->second == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query kind amount: decrease to negative on '{}', {}", query_kind, found->second);
    else
        found->second -= 1;
}

ProcessList::QueryAmount ProcessList::getQueryKindAmount(const IAST::QueryKind & query_kind) const
{
    auto found = query_kind_amounts.find(query_kind);
    if (found == query_kind_amounts.end())
        return 0;
    return found->second;
}

void ProcessList::increaseWaitingQueryAmount(const QueryStatusPtr & status)
{
    UInt64 limit = max_waiting_queries_amount.load();
    UInt64 value = waiting_queries_amount.load();
    while (true)
    {
        if (value >= limit)
            throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                            "Too many simultaneous waiting queries. Maximum: {}, waiting: {}",
                            limit, value);
        if (waiting_queries_amount.compare_exchange_strong(value, value + 1))
            break;
    }

    // WARNING: After this point we should not throw, otherwise corresponding `decreaseWaitingQueryAmount` will not be called.

    // Update query kind counters
    if (status->query_kind == IAST::QueryKind::Insert)
        waiting_insert_queries_amount.fetch_add(1);
    if (status->query_kind == IAST::QueryKind::Select)
        waiting_select_queries_amount.fetch_add(1);

    // Update per-user counter
    status->getUserProcessList()->waiting_queries_amount.fetch_add(1);

    // We have to notify because some queries might be waiting on `have_space`
    // and this query leaves its space by transitioning to waiting state
    have_space.notify_all();
}

void ProcessList::decreaseWaitingQueryAmount(const QueryStatusPtr & status)
{
    if (status->getUserProcessList()->waiting_queries_amount.fetch_sub(1) == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong insert waiting query amount for user: decrease to negative");

    if (status->query_kind == IAST::QueryKind::Insert && waiting_insert_queries_amount.fetch_sub(1) == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong insert waiting query amount: decrease to negative");

    if (status->query_kind == IAST::QueryKind::Select && waiting_select_queries_amount.fetch_sub(1) == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong select waiting query amount: decrease to negative");

    if (waiting_queries_amount.fetch_sub(1) == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong waiting query amount: decrease to negative");
}

void ProcessList::incrementWaiters()
{
    ContextPtr context = CurrentThread::getQueryContext();
    QueryStatusPtr status = context->getProcessListElement();

    // Query became "waiting" with the first thread that waits
    if (status->waiting_threads.fetch_add(1) == 0)
        increaseWaitingQueryAmount(status);
}

void ProcessList::decrementWaiters()
{
    ContextPtr context = CurrentThread::getQueryContext();
    QueryStatusPtr status = context->getProcessListElement();

    // Query became "non-waiting" with the last thread that no longer waits
    if (status->waiting_threads.fetch_sub(1) == 1)
        decreaseWaitingQueryAmount(status);
}

}
