#include <Interpreters/ProcessList.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/CancellationChecker.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/queryNormalization.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <base/scope_guard.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/OvercommitTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/logger_useful.h>
#include <chrono>
#include <memory>


namespace CurrentMetrics
{
    extern const Metric Query;
    extern const Metric QueryNonInternal;
    extern const Metric QueryAdmissionQueueLength;
}

namespace ProfileEvents
{
    extern const Event QueryAdmissionQueueWaitMicroseconds;
    extern const Event UserThrottlerBytes;
    extern const Event UserThrottlerSleepMicroseconds;
    extern const Event AllUsersThrottlerBytes;
    extern const Event AllUsersThrottlerSleepMicroseconds;
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
    extern const SettingsUInt64 memory_overcommit_ratio_denominator_for_user;
    extern const SettingsUInt64 max_temporary_data_on_disk_size_for_query;
    extern const SettingsUInt64 priority;
    extern const SettingsMilliseconds queue_max_wait_ms;
    extern const SettingsBool replace_running_query;
    extern const SettingsMilliseconds replace_running_query_max_wait_ms;
    extern const SettingsString temporary_files_codec;
    extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
    extern const SettingsOverflowMode timeout_overflow_mode;
    extern const SettingsBool trace_profile_events;
    extern const SettingsString trace_profile_events_list;
    extern const SettingsMilliseconds low_priority_query_wait_time_ms;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 admission_queue_alive_check_interval_ms;
}

namespace ErrorCodes
{
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int ARGUMENT_OUT_OF_BOUND;
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

ProcessList::EntryPtr ProcessList::insert(
    const String & query_,
    UInt64 normalized_query_hash,
    const IAST * ast,
    ContextMutablePtr query_context,
    UInt64 watch_start_nanoseconds,
    bool is_internal)
{
    EntryPtr res;

    const ClientInfo & client_info = query_context->getClientInfo();
    const Settings & settings = query_context->getSettingsRef();

    if (client_info.current_query_id.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query id cannot be empty");

    bool is_unlimited_query = isUnlimitedQuery(ast) || is_internal;
    std::shared_ptr<QueryStatus> query;

    // Acquire a query slot from resource scheduler if necessary.
    // NOTE: There is a separate independent limit for the whole server `max_concurrent_queries`.
    // NOTE: If that limit is exhausted, the query will be later blocked and wait while holding a query slot.
    QuerySlotPtr query_slot;
    if (!is_unlimited_query)
    {
        String query_resource_name = query_context->getWorkloadEntityStorage().getQueryResourceName();
        if (!query_resource_name.empty())
        {
            if (ResourceLink link = query_context->getWorkloadClassifier()->get(query_resource_name))
                query_slot = std::make_unique<QuerySlot>(link);
        }
    }

    bool got_admission_slot = false;

    {
        LockAndOverCommitTrackerBlocker<std::unique_lock, Mutex> locker(mutex); /// To avoid deadlock in case of OOM
        auto & lock = locker.getUnderlyingLock();
        IAST::QueryKind query_kind = ast ? ast->getQueryKind() : IAST::QueryKind::Select;

        const auto queue_max_wait_ms = settings[Setting::queue_max_wait_ms].totalMilliseconds();

        /// --- FIFO admission control (see ProcessList.h for design overview) ---
        bool needs_admission = !is_unlimited_query && max_size && admission_queue_enabled;

        if (needs_admission && admission_running >= max_size)
        {
            /// Timeout fallback: queue_max_wait_ms → max_execution_time → default receive timeout (300s).
            UInt64 effective_wait_ms = queue_max_wait_ms
                ? queue_max_wait_ms
                : settings[Setting::max_execution_time].totalMilliseconds();

            if (effective_wait_ms == 0)
                effective_wait_ms = DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC * 1000;

            auto is_alive = query_context->getConnectionAliveCheck();
            const auto alive_check_interval_ms = std::clamp(static_cast<UInt64>(
                query_context->getServerSettings()[ServerSetting::admission_queue_alive_check_interval_ms]),
                UInt64(500), UInt64(10000));

            /// Stack-allocate a waiter and enqueue it (FIFO).
            AdmissionWaiter waiter;
            admission_queue.push_back(&waiter);

            /// Guard: on exception, clean up our deque entry or release a
            /// slot that was granted to us just before the throw.
            SCOPE_EXIT({
                if (!got_admission_slot)
                {
                    if (waiter.granted)
                    {
                        /// We were granted but are throwing — release the slot.
                        releaseAdmissionSlotLocked(lock);
                    }
                    else
                    {
                        std::erase(admission_queue, &waiter);
                    }
                }
            });

            CurrentMetrics::Increment queue_length_increment(CurrentMetrics::QueryAdmissionQueueLength);
            Stopwatch admission_watch;

            /// Predicate: the releaser has granted us the slot.
            auto my_turn = [&] { return waiter.granted; };

            if (is_alive && alive_check_interval_ms > 0)
            {
                /// Periodic alive-check loop.
                auto deadline = std::chrono::steady_clock::now()
                    + std::chrono::milliseconds(effective_wait_ms);

                while (!my_turn())
                {
                    auto now = std::chrono::steady_clock::now();
                    if (now >= deadline)
                    {
                        throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                        "Too many simultaneous queries. Maximum: {}. Waited {} ms.",
                                        max_size, effective_wait_ms);
                    }

                    auto chunk = std::min(
                        std::chrono::milliseconds(alive_check_interval_ms),
                        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));

                    waiter.cv.wait_for(lock, chunk, my_turn);

                    if (!my_turn() && is_alive && !is_alive())
                    {
                        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                                        "Query admission cancelled: client disconnected while waiting in queue");
                    }
                }
            }
            else
            {
                /// Simple wait with timeout, no alive check.
                if (!waiter.cv.wait_for(lock, std::chrono::milliseconds(effective_wait_ms), my_turn))
                {
                    throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                    "Too many simultaneous queries. Maximum: {}. Waited {} ms.",
                                    max_size, effective_wait_ms);
                }
            }

            ProfileEvents::increment(ProfileEvents::QueryAdmissionQueueWaitMicroseconds, admission_watch.elapsedMicroseconds());

            /// Final alive-check after the slot has been granted but before we commit to using it.
            /// The periodic loop above only checks liveness while still waiting (`!my_turn()`). If the
            /// releaser transferred the slot to us between two periodic checks, a client that disconnected
            /// in the meantime would otherwise be admitted and execute its query. Re-check here so that a
            /// disconnected waiter is cancelled before consuming the slot; the `SCOPE_EXIT` above releases
            /// the already-granted slot when we throw.
            if (is_alive && !is_alive())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                                "Query admission cancelled: client disconnected while waiting in queue");

            /// Slot was transferred to us by the releaser (no counter change).
            got_admission_slot = true;
        }
        else if (needs_admission)
        {
            /// Slot available immediately — take it.
            ++admission_running;
            got_admission_slot = true;
        }

        /// Guard: if any subsequent check throws (max_insert_queries_amount,
        /// max_concurrent_queries_for_all_users, etc.), release the admission slot.
        scope_guard admission_rollback([&]
        {
            if (got_admission_slot)
            {
                releaseAdmissionSlotLocked(lock);
                got_admission_slot = false;
                /// If `QueryStatus` was already constructed, also clear its flag so that a
                /// destructing `ProcessListEntry` does not release the same slot a second time.
                if (query)
                    query->holds_admission_slot = false;
            }
        });

        if (!is_unlimited_query && max_size && !admission_queue_enabled)
        {
            /// Legacy path: broadcast condvar (admission queue disabled).
            if (non_internal_processes >= max_size)
            {
                if (queue_max_wait_ms)
                    LOG_WARNING(getLogger("ProcessList"), "Too many simultaneous queries, will wait {} ms.", queue_max_wait_ms);
                if (!queue_max_wait_ms || !query_finished.wait_for(lock, std::chrono::milliseconds(queue_max_wait_ms),
                        [&]{ return non_internal_processes < max_size; }))
                    throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                    "Too many simultaneous queries. Maximum: {}",
                                    max_size);
            }
        }

        if (!is_unlimited_query)
        {
            QueryAmount amount = getQueryKindAmount(query_kind);
            if (max_insert_queries_amount && query_kind == IAST::QueryKind::Insert && amount >= max_insert_queries_amount)
                throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                "Too many simultaneous insert queries. Maximum: {}, current: {}",
                                max_insert_queries_amount, amount);
            if (max_select_queries_amount && query_kind == IAST::QueryKind::Select && amount >= max_select_queries_amount)
                throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                "Too many simultaneous select queries. Maximum: {}, current: {}",
                                max_select_queries_amount, amount);
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

            if (!is_unlimited_query && settings[Setting::max_concurrent_queries_for_all_users]
                && non_internal_processes >= settings[Setting::max_concurrent_queries_for_all_users])
                throw Exception(
                    ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                    "Too many simultaneous queries for all users. "
                    "Current: {}, maximum: {}",
                    non_internal_processes,
                    settings[Setting::max_concurrent_queries_for_all_users].toString());
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
                if (!is_unlimited_query && settings[Setting::max_concurrent_queries_for_user]
                    && user_process_list->second.non_internal_queries >= settings[Setting::max_concurrent_queries_for_user])
                    throw Exception(
                        ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                        "Too many simultaneous queries for user {}. "
                        "Current: {}, maximum: {}",
                        client_info.current_user,
                        user_process_list->second.non_internal_queries,
                        settings[Setting::max_concurrent_queries_for_user].toString());

                auto running_query = user_process_list->second.queries.find(client_info.current_query_id);

                if (running_query != user_process_list->second.queries.end())
                {
                    if (!settings[Setting::replace_running_query])
                        throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING, "Query with id = {} is already running.", client_info.current_query_id);

                    /// Ask queries to cancel. They will check this flag.
                    running_query->second->is_killed.store(true, std::memory_order_relaxed);

                    const auto replace_running_query_max_wait_ms = settings[Setting::replace_running_query_max_wait_ms].totalMilliseconds();
                    if (!replace_running_query_max_wait_ms || !query_finished.wait_for(lock, std::chrono::milliseconds(replace_running_query_max_wait_ms),
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
            thread_group->performance_counters.setUserCounters(&user_process_list.user_performance_counters);
            thread_group->memory_tracker.setParent(&user_process_list.user_memory_tracker);
            if (user_process_list.user_temp_data_on_disk)
            {
                TemporaryDataOnDiskSettings temporary_data_on_disk_settings
                {
                    .max_size_on_disk = settings[Setting::max_temporary_data_on_disk_size_for_query],
                    .compression_codec = settings[Setting::temporary_files_codec],
                    .buffer_size = settings[Setting::temporary_files_buffer_size],
                    .metrics = {}, /// Metrics are set by child scopes
                };

                if (temporary_data_on_disk_settings.buffer_size > 1_GiB)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Too large `temporary_files_buffer_size`, maximum 1 GiB");

                if (user_process_list.user_temp_data_on_disk)
                    query_context->setTempDataOnDisk(std::make_shared<TemporaryDataOnDiskScope>(
                        user_process_list.user_temp_data_on_disk, std::move(temporary_data_on_disk_settings)));
            }

            /// Set query-level memory trackers
            thread_group->memory_tracker.setOrRaiseHardLimit(settings[Setting::max_memory_usage]);
            configureMemoryTrackerFromSettings(query_context->hasTraceCollector(), thread_group->memory_tracker, settings);

            if (query_context->hasTraceCollector() && settings[Setting::trace_profile_events])
            {
                const String & list_of_events_to_trace = settings[Setting::trace_profile_events_list];
                if (!list_of_events_to_trace.empty())
                {
                    /// Trace specific profile events
                    thread_group->performance_counters.setTraceProfileEvents(list_of_events_to_trace);
                }
                else
                {
                    /// Trace all profile events
                    thread_group->performance_counters.setTraceAllProfileEvents();
                }
            }

            thread_group->memory_tracker.setDescription("Query");
            thread_group->memory_tracker.setOvercommitWaitingTime(settings[Setting::memory_usage_overcommit_max_wait_microseconds]);

            /// NOTE: Do not set the limit for thread-level memory tracker since it could show unreal values
            ///  since allocation and deallocation could happen in different threads
        }

        query = std::make_shared<QueryStatus>(
            query_context,
            query_,
            normalized_query_hash,
            client_info,
            priorities.insert(
                settings[Setting::priority],
                std::chrono::milliseconds(settings[Setting::low_priority_query_wait_time_ms].totalMilliseconds())),
            std::move(query_slot),
            got_admission_slot,
            std::move(thread_group),
            query_kind,
            settings,
            watch_start_nanoseconds,
            is_internal);

        auto process_it = processes.emplace(
            processes.end(),
            query);

        /// We should not include internal queries for limiting the number of simultaneous queries.
        if (!is_internal)
        {
            ++non_internal_processes;
            increaseQueryKindAmount(query_kind);
        }

        bool registered_in_cancellation_checker = CancellationChecker::getInstance().appendTask(query, query_context->getSettingsRef()[Setting::max_execution_time].totalMilliseconds(), query_context->getSettingsRef()[Setting::timeout_overflow_mode]);

        res = std::make_shared<Entry>(*this, process_it, registered_in_cancellation_checker);

        (*process_it)->setUserProcessList(&user_process_list);
        (*process_it)->setProcessListEntry(res);

        user_process_list.queries.emplace(client_info.current_query_id, res->getQueryStatus());
        queries_to_user.emplace(client_info.current_query_id, client_info.current_user);
        if (!is_internal)
        {
            ++user_process_list.non_internal_queries;
        }

        /// Track memory usage for all simultaneously running queries from single user.
        user_process_list.user_memory_tracker.setOrRaiseHardLimit(settings[Setting::max_memory_usage_for_user]);
        user_process_list.user_memory_tracker.setSoftLimit(settings[Setting::memory_overcommit_ratio_denominator_for_user]);
        user_process_list.user_memory_tracker.setDescription("User");

        if (!total_network_throttler && settings[Setting::max_network_bandwidth_for_all_users])
        {
            total_network_throttler = std::make_shared<Throttler>(
                settings[Setting::max_network_bandwidth_for_all_users],
                ProfileEvents::AllUsersThrottlerBytes,
                ProfileEvents::AllUsersThrottlerSleepMicroseconds);
        }

        if (!user_process_list.user_throttler)
        {
            if (settings[Setting::max_network_bandwidth_for_user])
                user_process_list.user_throttler = std::make_shared<Throttler>(
                    settings[Setting::max_network_bandwidth_for_user],
                    total_network_throttler,
                    ProfileEvents::UserThrottlerBytes,
                    ProfileEvents::UserThrottlerSleepMicroseconds);
            else if (settings[Setting::max_network_bandwidth_for_all_users])
                user_process_list.user_throttler = total_network_throttler;
        }

        /// The query is now fully registered: from here on the admission slot is owned by the
        /// constructed `ProcessListEntry` (released in its destructor, or earlier in `executeQuery`).
        /// Only now is it safe to dismiss the rollback guard — if any step above threw after the
        /// guard was armed (e.g. an allocation failure in `processes.emplace`, `appendTask`, or the
        /// `Entry` construction), the guard releases the slot and prevents it from leaking.
        admission_rollback.release();
    }

    return res;
}


ProcessListEntry::~ProcessListEntry()
{
    if (registered_in_cancellation_checker)
    {
        /// We need to block the overcommit tracker here to avoid lock inversion because OvercommitTracker takes a lock on the ProcessList::mutex.
        /// When task is added, we lock the ProcessList::mutex, and then the CancellationChecker mutex.
        OvercommitTrackerBlockerInThread blocker;
        CancellationChecker::getInstance().appendDoneTasks(*it);
    }

    LockAndOverCommitTrackerBlocker<std::unique_lock, ProcessList::Mutex> lock(parent.getMutex());

    const String user = (*it)->getClientInfo().current_user;
    const String query_id = (*it)->getClientInfo().current_query_id;
    const IAST::QueryKind query_kind = (*it)->query_kind;
    const bool is_internal = (*it)->isInternal();

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
            if (!is_internal)
            {
                --user_process_list.non_internal_queries;
            }
            found = true;
        }
    }

    /// Wait for the query if it is in the cancellation right now.
    parent.cancelled_cv.wait(lock.getUnderlyingLock(), [&]() { return process_list_element_ptr->is_cancelling == false; });

    if (auto query_user = parent.queries_to_user.find(query_id); query_user != parent.queries_to_user.end())
        parent.queries_to_user.erase(query_user);

    /// This removes the memory_tracker of one request.
    parent.processes.erase(it);

    if (!is_internal)
    {
        --parent.non_internal_processes;
        parent.decreaseQueryKindAmount(query_kind);
    }

    if (!found)
    {
        LOG_ERROR(getLogger("ProcessList"), "Cannot find query by query_id and pointer to ProcessListElement in ProcessListForUser");
        std::terminate();
    }

    /// Release admission slot (if still held) via the FIFO mechanism.
    if (process_list_element_ptr->holds_admission_slot)
    {
        process_list_element_ptr->holds_admission_slot = false;
        parent.releaseAdmissionSlotLocked(lock.getUnderlyingLock());
    }

    /// Broadcast to `replace_running_query` waiters (they use `query_finished`).
    parent.query_finished.notify_all();

    /// If there are no more queries for the user, then we will reset memory tracker.
    if (user_process_list.queries.empty())
        user_process_list.resetTrackers();
}


QueryStatus::QueryStatus(
    ContextPtr context_,
    const String & query_,
    UInt64 normalized_query_hash_,
    const ClientInfo & client_info_,
    QueryPriorities::Handle && priority_handle_,
    QuerySlotPtr && query_slot_,
    bool holds_admission_slot_,
    ThreadGroupPtr && thread_group_,
    IAST::QueryKind query_kind_,
    const Settings & query_settings_,
    UInt64 watch_start_nanoseconds,
    bool is_internal_)
    : WithContext(context_)
    , query(query_)
    , normalized_query_hash(normalized_query_hash_)
    , client_info(client_info_)
    , query_slot(std::move(query_slot_))
    , holds_admission_slot(holds_admission_slot_)
    , thread_group(std::move(thread_group_))
    , watch(CLOCK_MONOTONIC, watch_start_nanoseconds, true)
    , priority_handle(std::move(priority_handle_))
    , global_overcommit_tracker(context_->getGlobalOvercommitTracker())
    , query_kind(query_kind_)
    , num_queries_increment(CurrentMetrics::Query)
    , is_internal(is_internal_)
{
    if (!is_internal)
        num_non_internal_queries_increment.emplace(CurrentMetrics::QueryNonInternal);

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
        chassert(!e->executor);
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

CancellationCode QueryStatus::cancelQuery(CancelReason reason, std::exception_ptr exception)
{
    {
        std::lock_guard<std::mutex> lock(cancel_mutex);

        if (is_killed)
            return CancellationCode::CancelSent;

        LOG_TRACE(getLogger("ProcessList"), "Cancelling the query (reason: {})", reason);

        is_killed = true;
        cancel_reason = reason;
        cancellation_exception = exception;
    }

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

void QueryStatus::throwProperExceptionIfNeeded(const UInt64 & max_execution_time_ms, const UInt64 & elapsed_ns)
{
    {
        std::lock_guard<std::mutex> lock(cancel_mutex);
        if (is_killed)
        {
            String additional_error_part;
            if (elapsed_ns)
                additional_error_part = fmt::format("elapsed {} ms, ", static_cast<double>(elapsed_ns) / 1000000ULL);

            if (cancel_reason == CancelReason::TIMEOUT)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded: {}maximum: {} ms", additional_error_part, max_execution_time_ms);
            throwQueryWasCancelled();
        }
    }
}

void QueryStatus::addPipelineExecutor(PipelineExecutor * e)
{
    /// In case of asynchronous distributed queries it is possible to call
    /// addPipelineExecutor() from the cancelQuery() context, and this will
    /// lead to deadlock.
    UInt64 max_exec_time = getContext()->getSettingsRef()[Setting::max_execution_time].totalMilliseconds();
    throwProperExceptionIfNeeded(max_exec_time, 0);

    std::lock_guard lock(executors_mutex);
    chassert(!executors.contains(e));
    executors[e] = std::make_shared<ExecutorHolder>(e);
}

void QueryStatus::removePipelineExecutor(PipelineExecutor * e)
{
    ExecutorHolderPtr executor_holder;

    {
        std::lock_guard lock(executors_mutex);
        chassert(executors.contains(e));
        executor_holder = executors[e];
        executors.erase(e);
    }

    /// Invalidate executor pointer inside holder.
    /// We should do it with released executors_mutex to avoid possible lock order inversion.
    executor_holder->remove();
}

bool QueryStatus::checkTimeLimit()
{
    auto elapsed_ns = watch.elapsed();
    throwProperExceptionIfNeeded(limits.max_execution_time.totalMilliseconds(), elapsed_ns);

    return limits.checkTimeLimit(elapsed_ns, overflow_mode);
}

void QueryStatus::throwQueryWasCancelled() const
{
    if (cancellation_exception)
        std::rethrow_exception(cancellation_exception);
    else
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
}

void QueryStatus::throwIfKilled()
{
    if (!is_killed.load())
        return;
    throwProperExceptionIfNeeded(limits.max_execution_time.totalMilliseconds(), 0);
}

CancelReason QueryStatus::getCancelReason() const
{
    std::lock_guard<std::mutex> lock(cancel_mutex);
    return cancel_reason;
}

bool QueryStatus::checkTimeLimitSoft()
{
    if (is_killed.load())
        return false;

    return limits.checkTimeLimit(watch.elapsedNanoseconds(), OverflowMode::BREAK);
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

MemoryTracker * QueryStatus::getMemoryTracker() const
{
    if (!thread_group)
        return nullptr;
    return &thread_group->memory_tracker;
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


CancellationCode ProcessList::sendCancelToQuery(const String & current_query_id, const String & current_user)
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
        LockAndBlocker lock(mutex);
        elem = tryGetProcessListElement(current_query_id, current_user);
        if (!elem)
            return CancellationCode::NotFound;
        elem->is_cancelling = true;
    }

    SCOPE_EXIT({
        DENY_ALLOCATIONS_IN_SCOPE;

        Lock lock(mutex);
        elem->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    return elem->cancelQuery(CancelReason::CANCELLED_BY_USER);
}


CancellationCode ProcessList::sendCancelToQuery(QueryStatusPtr elem)
{
    /// Cancelling the query should be done without the lock.
    /// So here we first set is_cancelling, and later reset it.
    /// The ProcessListEntry cannot be destroy if is_cancelling is true.
    {
        LockAndBlocker lock(mutex);
        elem->is_cancelling = true;
    }

    SCOPE_EXIT({
        DENY_ALLOCATIONS_IN_SCOPE;

        Lock lock(mutex);
        elem->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    return elem->cancelQuery(CancelReason::CANCELLED_BY_USER);
}


void ProcessList::killAllQueries()
{
    std::vector<QueryStatusPtr> cancelled_processes;

    SCOPE_EXIT({
        LockAndBlocker lock(mutex);
        for (auto & cancelled_process : cancelled_processes)
            cancelled_process->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    {
        LockAndBlocker lock(mutex);
        cancelled_processes.reserve(processes.size());
        for (auto & process : processes)
        {
            cancelled_processes.push_back(process);
            process->is_cancelling = true;
        }
    }

    for (auto & cancelled_process : cancelled_processes)
        cancelled_process->cancelQuery(CancelReason::CANCELLED_BY_USER);

}

bool QueryStatus::updateProgressIn(const Progress & value)
{
    CurrentThread::updateProgressIn(value);
    progress_in.incrementPiecewiseAtomically(value);

    if (priority_handle)
        priority_handle->waitIfNeed();

    return !is_killed.load(std::memory_order_relaxed);
}

bool QueryStatus::updateProgressOut(const Progress & value)
{
    CurrentThread::updateProgressOut(value);
    progress_out.incrementPiecewiseAtomically(value);

    return !is_killed.load(std::memory_order_relaxed);
}


QueryStatusInfo QueryStatus::getInfo(bool get_thread_list, bool get_profile_events, bool get_settings) const
{
    QueryStatusInfo res{};

    res.query             = query;
    res.normalized_query_hash = normalized_query_hash;
    res.query_kind        = query_kind;
    res.client_info       = client_info;
    res.elapsed_microseconds = watch.elapsedMicroseconds();
    res.is_cancelled      = is_killed.load(std::memory_order_relaxed);
    res.is_all_data_sent  = is_all_data_sent.load(std::memory_order_relaxed);
    res.is_internal       = is_internal;
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
        LockAndBlocker lock(mutex);
        processes_copy.assign(processes.begin(), processes.end());
    }

    Info per_query_infos;
    per_query_infos.reserve(processes_copy.size());
    for (const auto & process : processes_copy)
        per_query_infos.emplace_back(process->getInfo(get_thread_list, get_profile_events, get_settings));

    return per_query_infos;
}

QueryStatusPtr ProcessList::getProcessListElement(const String & query_id) const
{
    LockAndBlocker lock(mutex);
    for (const auto & process : processes)
    {
        if (process->client_info.current_query_id == query_id)
            return process;
    }

    return nullptr;
}

QueryStatusInfoPtr ProcessList::getQueryInfo(const String & query_id, bool get_thread_list, bool get_profile_events, bool get_settings) const
{
    auto process = getProcessListElement(query_id);
    if (process)
        return std::make_shared<QueryStatusInfo>(process->getInfo(get_thread_list, get_profile_events, get_settings));

    return nullptr;
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
            .compression_codec = settings[Setting::temporary_files_codec],
            .buffer_size = settings[Setting::temporary_files_buffer_size],
            .metrics = {}, /// Metrics are set by child scopes
        };

        if (auto shared_temp_data = global_context->getSharedTempDataOnDisk())
            user_temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(std::move(shared_temp_data),
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

    LockAndBlocker lock(mutex);

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
    if (found->second == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query kind amount: decrease to negative on '{}', {}", query_kind, found->second);
    found->second -= 1;
}

ProcessList::QueryAmount ProcessList::getQueryKindAmount(const IAST::QueryKind & query_kind) const
{
    auto found = query_kind_amounts.find(query_kind);
    if (found == query_kind_amounts.end())
        return 0;
    return found->second;
}

/// --- Admission control helpers ---

void ProcessList::releaseAdmissionSlotLocked(Lock & /* acquired_lock */)
{
    chassert(admission_running > 0);

    /// Respect runtime decreases of `max_concurrent_queries`: when we are over
    /// the current limit, decrement instead of transferring so the new (smaller)
    /// limit takes effect. The next release that brings `admission_running` back
    /// to `max_size` will hand the slot to the front waiter.
    if (admission_queue.empty() || admission_running > max_size)
    {
        --admission_running;
        return;
    }

    /// Transfer the slot to the front waiter (FIFO).
    AdmissionWaiter * front = admission_queue.front();
    admission_queue.pop_front();
    front->granted = true;
    front->cv.notify_one();
}

void ProcessList::setMaxSize(size_t max_size_)
{
    Lock lock(mutex);

    max_size = max_size_;

    /// Drain queued waiters that the new limit now admits. Without this,
    /// raising `max_concurrent_queries` (or setting it to 0/unlimited) would
    /// leave existing waiters blocked until their `queue_max_wait_ms` timeout,
    /// because finishing queries decrement `admission_running` instead of
    /// transferring slots (see `releaseAdmissionSlotLocked`) and new queries
    /// bypass admission entirely when `max_size == 0`.
    while (!admission_queue.empty() && (max_size == 0 || admission_running < max_size))
    {
        AdmissionWaiter * front = admission_queue.front();
        admission_queue.pop_front();
        ++admission_running;
        front->granted = true;
        front->cv.notify_one();
    }
}

void QueryStatus::releaseAdmissionSlot()
{
    if (!holds_admission_slot)
        return;

    /// We need ProcessList::mutex because all admission fields are guarded by it.
    auto entry = getProcessListEntry();
    if (!entry)
        return;

    ProcessList & process_list = entry->getProcessList();
    {
        LockAndOverCommitTrackerBlocker<std::unique_lock, ProcessList::Mutex> locker(process_list.getMutex());
        auto & lock = locker.getUnderlyingLock();

        if (!holds_admission_slot)
            return; /// Double-check under lock.

        holds_admission_slot = false;
        process_list.releaseAdmissionSlotLocked(lock);
    }
}

}
