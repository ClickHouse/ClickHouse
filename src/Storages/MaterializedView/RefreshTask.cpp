#include <Storages/MaterializedView/RefreshTask.h>

#include <Common/CurrentMetrics.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Common/Macros.h>
#include <Common/thread_local_rng.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/ServerSettings.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/queryNormalization.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Storages/StorageMaterializedView.h>


namespace CurrentMetrics
{
    extern const Metric RefreshingViews;
}

namespace ProfileEvents
{
    extern const Event RefreshableViewRefreshSuccess;
    extern const Event RefreshableViewRefreshFailed;
    extern const Event RefreshableViewSyncReplicaSuccess;
    extern const Event RefreshableViewSyncReplicaRetry;
    extern const Event RefreshableViewLockTableRetry;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 log_queries_cut_to_length;
    extern const SettingsBool stop_refreshable_materialized_views_on_startup;
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ServerSetting
{
    extern const ServerSettingsString default_replica_name;
    extern const ServerSettingsString default_replica_path;
    extern const ServerSettingsBool disable_insertion_and_mutation;
}

namespace RefreshSetting
{
    extern const RefreshSettingsBool all_replicas;
    extern const RefreshSettingsInt64 refresh_retries;
    extern const RefreshSettingsUInt64 refresh_retry_initial_backoff_ms;
    extern const RefreshSettingsUInt64 refresh_retry_max_backoff_ms;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
    extern const int REFRESH_FAILED;
    extern const int TABLE_IS_DROPPED;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
    extern const int ABORTED;
}

RefreshTask::RefreshTask(
    StorageMaterializedView * view_, ContextPtr context, const DB::ASTRefreshStrategy & strategy, bool attach, bool coordinated, bool empty, bool is_restore_from_backup)
    : log(getLogger("RefreshTask"))
    , view(view_)
    , refresh_schedule(strategy)
    , refresh_append(strategy.append)
{
    if (strategy.settings != nullptr)
        refresh_settings.applyChanges(strategy.settings->changes);

    coordination.root_znode.randomize();
    if (empty)
        coordination.root_znode.last_completed_timeslot = std::chrono::floor<std::chrono::seconds>(currentTime());
    if (coordinated)
    {
        coordination.coordinated = true;

        const auto & server_settings = context->getServerSettings();
        const auto macros = context->getMacros();
        Macros::MacroExpansionInfo info;
        info.table_id = view->getStorageID();
        coordination.path = macros->expand(server_settings[ServerSetting::default_replica_path], info);
        coordination.replica_name = context->getMacros()->expand(server_settings[ServerSetting::default_replica_name], info);

        auto zookeeper = context->getZooKeeper();
        String replica_path = coordination.path + "/replicas/" + coordination.replica_name;
        bool replica_path_existed = zookeeper->exists(replica_path);

        /// Create znodes even if it's ATTACH query. This seems weird, possibly incorrect, but
        /// currently both DatabaseReplicated and DatabaseShared seem to require this behavior.
        if (!replica_path_existed)
        {
            if (!attach && !is_restore_from_backup &&
                !zookeeper->isFeatureEnabled(KeeperFeatureFlag::MULTI_READ))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Keeper server doesn't support multi-reads.");

            zookeeper->createAncestors(coordination.path);
            Coordination::Requests ops;
            ops.emplace_back(zkutil::makeCreateRequest(coordination.path, coordination.root_znode.toString(), zkutil::CreateMode::Persistent, /*ignore_if_exists*/ true));
            ops.emplace_back(zkutil::makeCreateRequest(coordination.path + "/replicas", "", zkutil::CreateMode::Persistent, true));
            ops.emplace_back(zkutil::makeCreateRequest(replica_path, "", zkutil::CreateMode::Persistent));

            /// When restoring multiple tables from backup (e.g. a RESTORE DATABASE), the restored
            /// refreshable materialized views shouldn't start refreshing on any replica until all
            /// tables and their data is restored on all replicas. Otherwise things break:
            ///  * Refresh may EXCHANGE+DROP a table before its data is restored. The restore will
            ///    then fail when trying to write to a dropped table.
            ///  * Refresh may see empty source table before they're restored, producing empty
            ///    refresh result.
            ///
            /// Note that with replicated catalog a replicated database may be restored
            /// by a RESTORE running on just one replica, so one replica needs to be able to unpause
            /// refreshes on all replicas. This is the only reason why "paused" znode is a thing,
            /// otherwise we could just use stop_requested.
            if (is_restore_from_backup)
                ops.emplace_back(zkutil::makeCreateRequest(coordination.path + "/paused", "restored from backup", zkutil::CreateMode::Persistent, /*ignore_if_exists*/ true));

            zookeeper->multi(ops);
        }

        if (server_settings[ServerSetting::disable_insertion_and_mutation])
            coordination.read_only = true;
    }
    else
    {
        if (is_restore_from_backup)
            scheduling.stop_requested = true;
    }
}

OwnedRefreshTask RefreshTask::create(
    StorageMaterializedView * view,
    ContextMutablePtr context,
    const DB::ASTRefreshStrategy & strategy,
    bool attach,
    bool coordinated,
    bool empty,
    bool is_restore_from_backup)
{
    auto task = std::make_shared<RefreshTask>(view, context, strategy, attach, coordinated, empty, is_restore_from_backup);

    task->refresh_task = context->getSchedulePool().createTask("RefreshTask",
        [self = task.get()] { self->refreshTask(); });

    if (strategy.dependencies)
        for (auto && dependency : strategy.dependencies->children)
            task->initial_dependencies.emplace_back(dependency->as<const ASTTableIdentifier &>());

    return OwnedRefreshTask(task);
}

bool RefreshTask::canCreateOrDropOtherTables() const
{
    return !refresh_append;
}

void RefreshTask::startup()
{
    if (view->getContext()->getSettingsRef()[Setting::stop_refreshable_materialized_views_on_startup])
        scheduling.stop_requested = true;
    auto inner_table_id = refresh_append ? std::nullopt : std::make_optional(view->getTargetTableId());
    view->getContext()->getRefreshSet().emplace(view->getStorageID(), inner_table_id, initial_dependencies, shared_from_this());

    std::lock_guard guard(mutex);
    scheduleRefresh(guard);
}

void RefreshTask::finalizeRestoreFromBackup()
{
    if (coordination.coordinated)
        startReplicated();
    else
        start();
}

void RefreshTask::shutdown()
{
    {
        std::lock_guard guard(mutex);

        if (view == nullptr)
            return; // already shut down

        scheduling.stop_requested = true;
        interruptExecution();
    }

    /// If we're in DatabaseReplicated, interrupt replicated CREATE/EXCHANGE/DROP queries in refresh task.
    /// Without this we can deadlock waiting for refresh_task because this shutdown happens from the same DDL thread for which CREATE/EXCHANGE/DROP wait.
    execution.cancel_ddl_queries.request_stop();

    /// Wait for the task to return and prevent it from being scheduled in future.
    refresh_task->deactivate();

    /// Remove from RefreshSet on DROP, without waiting for the IStorage to be destroyed.
    /// This matters because a table may get dropped and immediately created again with the same name,
    /// while the old table's IStorage still exists (pinned by ongoing queries).
    /// (Also, RefreshSet holds a shared_ptr to us.)
    std::lock_guard guard(mutex);
    set_handle.reset();

    view = nullptr;
}

void RefreshTask::drop(ContextPtr context)
{
    if (coordination.coordinated)
    {
        auto zookeeper = context->getZooKeeper();

        zookeeper->tryRemove(coordination.path + "/replicas/" + coordination.replica_name);

        /// Redundant, refreshTask() is supposed to clean up after itself, but let's be paranoid.
        removeRunningZnodeIfMine(zookeeper);

        /// If no replicas left, remove the coordination znode.
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeRemoveRequest(coordination.path + "/replicas", -1));
        String paused_path = coordination.path + "/paused";
        if (zookeeper->exists(paused_path))
            ops.emplace_back(zkutil::makeRemoveRequest(paused_path, -1));
        ops.emplace_back(zkutil::makeRemoveRequest(coordination.path, -1));
        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);
        if (responses[0]->error != Coordination::Error::ZNOTEMPTY && responses[0]->error != Coordination::Error::ZNONODE)
            zkutil::KeeperMultiException::check(code, ops, responses);
    }
}

void RefreshTask::rename(StorageID new_id, StorageID new_inner_table_id)
{
    std::lock_guard guard(mutex);
    if (set_handle)
        set_handle.rename(new_id, refresh_append ? std::nullopt : std::make_optional(new_inner_table_id));
}

void RefreshTask::checkAlterIsPossible(const DB::ASTRefreshStrategy & new_strategy)
{
    RefreshSettings s;
    if (new_strategy.settings)
        s.applyChanges(new_strategy.settings->changes);
    if (s[RefreshSetting::all_replicas] != refresh_settings[RefreshSetting::all_replicas])
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Altering setting 'all_replicas' is not supported.");
    if (new_strategy.append != refresh_append)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Adding or removing APPEND is not supported.");
}

void RefreshTask::alterRefreshParams(const DB::ASTRefreshStrategy & new_strategy)
{
    StorageID view_storage_id = StorageID::createEmpty();

    {
        std::lock_guard guard(mutex);

        refresh_schedule = RefreshSchedule(new_strategy);
        std::vector<StorageID> deps;
        if (new_strategy.dependencies)
            for (auto && dependency : new_strategy.dependencies->children)
                deps.emplace_back(dependency->as<const ASTTableIdentifier &>());

        /// Update dependency graph.
        if (set_handle)
            set_handle.changeDependencies(deps);

        scheduleRefresh(guard);
        scheduling.dependencies_satisfied_until = std::chrono::sys_seconds(std::chrono::seconds(-1));

        refresh_settings = {};
        if (new_strategy.settings != nullptr)
            refresh_settings.applyChanges(new_strategy.settings->changes);

        if (view)
            view_storage_id = view->getStorageID();
    }

    /// In case refresh period changed.
    if (view_storage_id)
    {
        const auto & refresh_set = Context::getGlobalContextInstance()->getRefreshSet();
        refresh_set.notifyDependents(view_storage_id);
    }
}

RefreshTask::Info RefreshTask::getInfo() const
{
    std::lock_guard guard(mutex);
    return Info {.view_id = set_handle.getID(), .state = state, .next_refresh_time = next_refresh_time, .znode = coordination.root_znode, .refresh_running = coordination.running_znode_exists, .progress = execution.progress.getValues(), .unexpected_error = scheduling.unexpected_error};
}

void RefreshTask::start()
{
    std::lock_guard guard(mutex);
    if (!std::exchange(scheduling.stop_requested, false))
        return;
    scheduling.unexpected_error = std::nullopt;
    scheduleRefresh(guard);
}

void RefreshTask::stop()
{
    std::lock_guard guard(mutex);
    if (std::exchange(scheduling.stop_requested, true))
        return;
    interruptExecution();
    scheduleRefresh(guard);
}

void RefreshTask::startReplicated()
{
    if (!coordination.coordinated)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Refreshable materialized view is not coordinated.");
    const auto zookeeper = view->getContext()->getZooKeeper();
    String path = coordination.path + "/paused";
    auto code = zookeeper->tryRemove(path);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
        throw Coordination::Exception::fromPath(code, path);
}

void RefreshTask::stopReplicated(const String & reason)
{
    if (!coordination.coordinated)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Refreshable materialized view is not coordinated.");
    const auto zookeeper = view->getContext()->getZooKeeper();
    String path = coordination.path + "/paused";
    auto code = zookeeper->tryCreate(path, reason, zkutil::CreateMode::Persistent);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        throw Coordination::Exception::fromPath(code, path);
}

void RefreshTask::run()
{
    std::lock_guard guard(mutex);
    if (std::exchange(scheduling.out_of_schedule_refresh_requested, true))
        return;
    scheduleRefresh(guard);
}

void RefreshTask::cancel()
{
    std::lock_guard guard(mutex);
    interruptExecution();
    scheduleRefresh(guard);
}

void RefreshTask::wait()
{
    auto throw_if_error = [&]
    {
        if (!view)
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "The table was dropped or detached");
        if (!coordination.running_znode_exists && !coordination.root_znode.last_attempt_succeeded && coordination.root_znode.last_attempt_time.time_since_epoch().count() != 0)
            throw Exception(ErrorCodes::REFRESH_FAILED,
                "Refresh failed{}: {}", coordination.coordinated ? " (on replica " + coordination.root_znode.last_attempt_replica + ")" : "",
                coordination.root_znode.last_attempt_error.empty() ? "Replica went away" : coordination.root_znode.last_attempt_error);
    };

    std::unique_lock lock(mutex);
    refresh_cv.wait(lock, [&] {
        return state != RefreshState::Running && state != RefreshState::Scheduling &&
            state != RefreshState::RunningOnAnotherReplica && (state == RefreshState::Disabled || !scheduling.out_of_schedule_refresh_requested);
    });
    throw_if_error();

    if (coordination.coordinated && !refresh_append)
    {
        /// Wait until we see the table produced by the latest refresh.
        while (true)
        {
            UUID expected_table_uuid = coordination.root_znode.last_success_table_uuid;
            StorageID storage_id = view->getTargetTableId();
            ContextPtr context = view->getContext();
            lock.unlock();

            /// (Can't use `view` here because shutdown() may unset it in parallel with us.)
            StoragePtr storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);
            if (storage && storage->getStorageID().uuid == expected_table_uuid)
                return;

            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            lock.lock();
            /// Re-check last_attempt_succeeded in case another refresh EXCHANGEd the table but failed to write its uuid to keeper.
            throw_if_error();
        }
    }
}

bool RefreshTask::tryJoinBackgroundTask(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock lock(mutex);

    execution.cancel_ddl_queries.request_stop();

    auto duration = deadline - std::chrono::steady_clock::now();
    /// (Manually clamping to 0 because the standard library used to have (and possibly still has?)
    ///  a bug that wait_until would wait forever if the timestamp is in the past.)
    duration = std::max(duration, std::chrono::steady_clock::duration(0));
    return refresh_cv.wait_for(lock, duration, [&]
        {
            return state != RefreshState::Running && state != RefreshState::Scheduling;
        });
}

std::chrono::sys_seconds RefreshTask::getNextRefreshTimeslot() const
{
    std::lock_guard guard(mutex);
    return refresh_schedule.advance(coordination.root_znode.last_completed_timeslot);
}

void RefreshTask::notify()
{
    std::lock_guard guard(mutex);
    if (view && view->getContext()->getRefreshSet().refreshesStopped())
        interruptExecution();
    scheduling.dependencies_satisfied_until = std::chrono::sys_seconds(std::chrono::seconds(-1));
    scheduleRefresh(guard);
}

void RefreshTask::setFakeTime(std::optional<Int64> t)
{
    std::unique_lock lock(mutex);
    scheduling.fake_clock.store(t.value_or(INT64_MIN), std::memory_order_relaxed);
    /// Reschedule task with shorter delay if currently scheduled.
    refresh_task->scheduleAfter(100, /*overwrite*/ true, /*only_if_scheduled*/ true);
}

void RefreshTask::refreshTask()
{
    std::unique_lock lock(mutex);

    auto schedule_keeper_retry = [&] {
        chassert(lock.owns_lock());
        chassert(state == RefreshState::Scheduling);
        coordination.watches->should_reread_znodes.store(true);
        refresh_task->scheduleAfter(5000);
    };

    try
    {
        bool refreshed_just_now = false;
        /// Whoever breaks out of this loop should assign state.
        while (true)
        {
            setState(RefreshState::Scheduling, lock);
            execution.interrupt_execution.store(false);

            updateDependenciesIfNeeded(lock);

            std::shared_ptr<zkutil::ZooKeeper> zookeeper;
            if (coordination.coordinated)
                zookeeper = view->getContext()->getZooKeeper();
            readZnodesIfNeeded(zookeeper, lock);
            chassert(lock.owns_lock());

            /// Check if another replica is already running a refresh.
            if (coordination.running_znode_exists)
            {
                if (coordination.root_znode.last_attempt_replica == coordination.replica_name)
                {
                    LOG_ERROR(log, "Znode {} indicates that this replica is running a refresh, but it isn't. Likely a bug.", coordination.path + "/running");
#ifdef DEBUG_OR_SANITIZER_BUILD
                    abortOnFailedAssertion("Unexpected refresh lock in keeper");
#else
                    coordination.running_znode_exists = false;
                    if (coordination.coordinated)
                        removeRunningZnodeIfMine(zookeeper);
                    schedule_keeper_retry();
                    break;
#endif
                }
                else
                {
                    setState(RefreshState::RunningOnAnotherReplica, lock);
                    break;
                }
            }

            chassert(lock.owns_lock());

            if (scheduling.stop_requested || coordination.paused_znode_exists || view->getContext()->getRefreshSet().refreshesStopped() || coordination.read_only)
            {
                /// Exit the task and wait for the user to start or resume, which will schedule the task again.
                setState(RefreshState::Disabled, lock);
                break;
            }

            /// Check if it's time to refresh.
            auto start_time = currentTime();
            auto start_time_seconds = std::chrono::floor<std::chrono::seconds>(start_time);
            Stopwatch stopwatch;
            auto [when, timeslot, start_znode] = determineNextRefreshTime(start_time_seconds);
            next_refresh_time = when;
            bool out_of_schedule = scheduling.out_of_schedule_refresh_requested;
            if (out_of_schedule)
            {
                chassert(start_znode.attempt_number > 0);
                start_znode.attempt_number -= 1;
            }
            else if (start_time < when)
            {
                size_t delay_ms = std::chrono::duration_cast<std::chrono::milliseconds>(when - start_time).count();
                /// If we're in a test that fakes the clock, poll every 100ms.
                if (scheduling.fake_clock.load(std::memory_order_relaxed) != INT64_MIN)
                    delay_ms = 100;
                refresh_task->scheduleAfter(delay_ms);
                setState(RefreshState::Scheduled, lock);
                break;
            }
            else if (timeslot >= scheduling.dependencies_satisfied_until)
            {
                setState(RefreshState::WaitingForDependencies, lock);
                break;
            }

            if (refreshed_just_now)
            {
                /// If doing two refreshes in a row, go through Scheduled state first,
                /// to give wait() a chance to complete.
                setState(RefreshState::Scheduled, lock);
                refresh_task->schedule();
                break;
            }

            /// Write to keeper.
            if (!updateCoordinationState(start_znode, true, zookeeper, lock))
            {
                schedule_keeper_retry();
                return;
            }
            chassert(lock.owns_lock());

            /// Perform a refresh.

            setState(RefreshState::Running, lock);
            scheduling.out_of_schedule_refresh_requested = false;
            bool append = refresh_append;
            int32_t root_znode_version = coordination.coordinated ? coordination.root_znode.version : -1;
            CurrentMetrics::Increment metric_inc(CurrentMetrics::RefreshingViews);

            String log_comment = fmt::format("refresh of {}", view->getStorageID().getFullTableName());
            if (start_znode.attempt_number > 1)
                log_comment += fmt::format(" (attempt {}/{})", start_znode.attempt_number, refresh_settings[RefreshSetting::refresh_retries] + 1);

            lock.unlock();

            String error_message;
            auto new_table_uuid = executeRefreshUnlocked(append, root_znode_version, start_time, stopwatch, log_comment, error_message);
            bool refreshed = new_table_uuid.has_value();

            lock.lock();

            setState(RefreshState::Scheduling, lock);

            auto end_time_seconds = std::chrono::floor<std::chrono::seconds>(currentTime());
            auto znode = coordination.root_znode;
            znode.last_attempt_time = end_time_seconds;
            znode.last_attempt_error = error_message;
            if (refreshed)
            {
                znode.last_attempt_succeeded = true;
                znode.last_completed_timeslot = refresh_schedule.timeslotForCompletedRefresh(znode.last_completed_timeslot, start_time_seconds, end_time_seconds, out_of_schedule);
                znode.last_success_time = start_time_seconds;
                znode.last_success_duration = std::chrono::milliseconds(stopwatch.elapsedMilliseconds());
                znode.last_success_table_uuid = *new_table_uuid;
                znode.previous_attempt_error = "";
                znode.attempt_number = 0;
                znode.randomize();
            }

            bool ok = updateCoordinationState(znode, false, zookeeper, lock);
            chassert(lock.owns_lock());
            if (!ok)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Refresh coordination znode was changed while refresh was in progress.");

            if (refreshed)
            {
                lock.unlock();
                view->getContext()->getRefreshSet().notifyDependents(view->getStorageID());
                lock.lock();
            }

            refreshed_just_now = true;
        }
    }
    catch (Coordination::Exception &)
    {
        tryLogCurrentException(log, "Keeper error");
        if (!lock.owns_lock())
            lock.lock();
        schedule_keeper_retry();
    }
    catch (...)
    {
        if (!lock.owns_lock())
            lock.lock();
        scheduling.stop_requested = true;
        scheduling.unexpected_error = getCurrentExceptionMessage(true);
        coordination.watches->should_reread_znodes.store(true);
        coordination.running_znode_exists = false;
        setState(RefreshState::Scheduling, lock);
        refresh_task->schedule();
        lock.unlock();

        tryLogCurrentException(log,
            "Exception in refresh scheduling. The view will be stopped.");
#ifdef DEBUG_OR_SANITIZER_BUILD
        /// There's at least one legitimate case where this may happen: if the user (DEFINER) was dropped.
        /// But it's unexpected in tests.
        /// Note that Coordination::Exception is caught separately above, so transient keeper errors
        /// don't go here and are just retried.
        abortOnFailedAssertion("Unexpected exception in refresh scheduling");
#else
        if (coordination.coordinated)
            removeRunningZnodeIfMine(view->getContext()->getZooKeeper());
#endif
    }
}

std::optional<UUID> RefreshTask::executeRefreshUnlocked(bool append, int32_t root_znode_version, std::chrono::system_clock::time_point start_time, const Stopwatch & stopwatch, const String & log_comment, String & out_error_message)
{
    StorageID view_storage_id = view->getStorageID();
    LOG_DEBUG(log, "Refreshing view {}", view_storage_id.getFullTableName());
    execution.progress.reset();

    ContextMutablePtr refresh_context;
    ProcessList::EntryPtr process_list_entry;
    std::optional<StorageID> table_to_drop;
    auto new_table_id = StorageID::createEmpty();

    std::optional<QueryLogElement> query_log_elem;
    std::shared_ptr<ASTInsertQuery> refresh_query;
    String query_for_logging;
    UInt64 normalized_query_hash = 0;
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span = std::make_shared<OpenTelemetry::SpanHolder>("query");

    try
    {
        refresh_context = view->createRefreshContext(log_comment);

        if (!append)
        {
            refresh_context->setParentTable(view_storage_id.uuid);
            refresh_context->setDDLQueryCancellation(execution.cancel_ddl_queries.get_token());
            if (root_znode_version != -1)
                refresh_context->setDDLAdditionalChecksOnEnqueue({zkutil::makeCheckRequest(coordination.path, root_znode_version)});
        }

        {
            /// Create a table.
            query_for_logging = "(create target table)";
            normalized_query_hash = normalizedQueryHash(query_for_logging, false);
            std::unique_ptr<CurrentThread::QueryScope> query_scope;
            std::tie(refresh_query, query_scope) = view->prepareRefresh(append, refresh_context, table_to_drop);
            new_table_id = refresh_query->table_id;

            /// Add the query to system.processes and allow it to be killed with KILL QUERY.
            query_for_logging = refresh_query->formatForLogging(
                refresh_context->getSettingsRef()[Setting::log_queries_cut_to_length]);
            normalized_query_hash = normalizedQueryHash(query_for_logging, false);

            process_list_entry = refresh_context->getProcessList().insert(
                query_for_logging, normalized_query_hash, refresh_query.get(), refresh_context, Stopwatch{CLOCK_MONOTONIC}.getStart());

            refresh_context->setProcessListElement(process_list_entry->getQueryStatus());
            refresh_context->setProgressCallback([this](const Progress & prog)
            {
                execution.progress.incrementPiecewiseAtomically(prog);
            });

            /// Run the query.

            InterpreterInsertQuery interpreter(
                refresh_query,
                refresh_context,
                /* allow_materialized */ false,
                /* no_squash */ false,
                /* no_destination */ false,
                /* async_isnert */ false);
            BlockIO block_io = interpreter.execute();
            QueryPipeline & pipeline = block_io.pipeline;

            /// We log the refresh as one INSERT SELECT query, but the timespan and exceptions also
            /// cover the surrounding CREATE, EXCHANGE, and DROP queries.
            query_log_elem = logQueryStart(
                start_time, refresh_context, query_for_logging, normalized_query_hash, refresh_query, pipeline,
                &interpreter, /*internal*/ false, view_storage_id.database_name,
                view_storage_id.table_name, /*async_insert*/ false);

            if (!pipeline.completed())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Pipeline for view {} refresh must be completed", view_storage_id.getFullTableName());

            PipelineExecutor executor(pipeline.processors, pipeline.process_list_element);
            executor.setReadProgressCallback(pipeline.getReadProgressCallback());

            {
                std::unique_lock exec_lock(execution.executor_mutex);
                if (execution.interrupt_execution.load())
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh for view {} cancelled", view_storage_id.getFullTableName());
                execution.executor = &executor;
            }
            SCOPE_EXIT({
                std::unique_lock exec_lock(execution.executor_mutex);
                execution.executor = nullptr;
            });

            executor.execute(pipeline.getNumThreads(), pipeline.getConcurrencyControl());

            /// A cancelled PipelineExecutor may return without exception but with incomplete results.
            /// In this case make sure to:
            ///  * report exception rather than success,
            ///  * do it before destroying the QueryPipeline; otherwise it may fail assertions about
            ///    being unexpectedly destroyed before completion and without uncaught exception
            ///    (specifically, the assert in ~WriteBuffer()).
            if (execution.interrupt_execution.load())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh for view {} cancelled", view_storage_id.getFullTableName());

            logQueryFinish(*query_log_elem, refresh_context, refresh_query, std::move(pipeline), /*pulling_pipeline=*/false, query_span, QueryResultCacheUsage::None, /*internal=*/false);
            query_log_elem = std::nullopt;
            query_span = nullptr;
        }

        /// Exchange tables.
        if (!append)
        {
            query_for_logging = "(exchange tables)";
            normalized_query_hash = normalizedQueryHash(query_for_logging, false);
            table_to_drop = view->exchangeTargetTable(new_table_id, refresh_context);
        }
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::RefreshableViewRefreshFailed);

        bool cancelled = execution.interrupt_execution.load();

        if (table_to_drop.has_value())
        {
            String discard_error_message;
            view->dropTempTable(table_to_drop.value(), refresh_context, discard_error_message);
        }

        if (query_log_elem.has_value())
        {
            logQueryException(*query_log_elem, refresh_context, stopwatch, refresh_query, query_span, /*internal*/ false, /*log_error*/ !cancelled);
        }
        else
        {
            /// Failed when creating new table or when swapping tables.
            logExceptionBeforeStart(query_for_logging, normalized_query_hash, refresh_context,
                                    /*ast*/ nullptr, query_span, stopwatch.elapsedMilliseconds());
        }

        if (cancelled)
            out_error_message = "cancelled";
        else
            out_error_message = getCurrentExceptionMessage(true);

        return std::nullopt;
    }

    ProfileEvents::increment(ProfileEvents::RefreshableViewRefreshSuccess);

    if (table_to_drop.has_value())
        view->dropTempTable(table_to_drop.value(), refresh_context, out_error_message);

    return new_table_id.uuid;
}

void RefreshTask::updateDependenciesIfNeeded(std::unique_lock<std::mutex> & lock)
{
    while (true)
    {
        chassert(lock.owns_lock());
        if (scheduling.dependencies_satisfied_until.time_since_epoch().count() >= 0)
            return;
        auto deps = set_handle.getDependencies();
        if (deps.empty())
        {
            scheduling.dependencies_satisfied_until = std::chrono::sys_seconds::max();
            return;
        }
        scheduling.dependencies_satisfied_until = std::chrono::sys_seconds(std::chrono::seconds(-2));
        lock.unlock();

        /// Consider a dependency satisfied if its next scheduled refresh time is greater than ours.
        /// This seems to produce reasonable behavior in practical cases, e.g.:
        ///  * REFRESH EVERY 1 DAY depends on REFRESH EVERY 1 DAY
        ///    The second refresh starts after the first refresh completes *for the same day*.
        ///  * REFRESH EVERY 1 DAY OFFSET 2 HOUR depends on REFRESH EVERY 1 DAY OFFSET 1 HOUR
        ///    The second refresh starts after the first refresh completes for the same day as well (scheduled 1 hour earlier).
        ///  * REFRESH EVERY 1 DAY OFFSET 1 HOUR depends on REFRESH EVERY 1 DAY OFFSET 23 HOUR
        ///    The dependency's refresh on day X triggers dependent's refresh on day X+1.
        ///  * REFRESH EVERY 2 HOUR depends on REFRESH EVERY 1 HOUR
        ///    The 2 HOUR refresh happens after the 1 HOUR refresh for every other hour, e.g.
        ///    after the 2pm refresh, then after the 4pm refresh, etc.
        ///
        /// We currently don't allow dependencies in REFRESH AFTER case, because its unclear what their meaning should be.

        const RefreshSet & set = view->getContext()->getRefreshSet();
        auto min_ts = std::chrono::sys_seconds::max();
        for (const StorageID & id : deps)
        {
            auto tasks = set.findTasks(id);
            if (tasks.empty())
                min_ts = {}; // missing table, dependency unsatisfied
            else
                min_ts = std::min(min_ts, (*tasks.begin())->getNextRefreshTimeslot());
        }

        lock.lock();

        if (scheduling.dependencies_satisfied_until.time_since_epoch().count() != -2)
        {
            /// Dependencies changed again after we started looking at them. Have to re-check.
            chassert(scheduling.dependencies_satisfied_until.time_since_epoch().count() == -1);
            continue;
        }

        scheduling.dependencies_satisfied_until = min_ts;
        return;
    }
}

static std::chrono::milliseconds backoff(Int64 retry_idx, const RefreshSettings & refresh_settings)
{
    UInt64 delay_ms;
    UInt64 multiplier = UInt64(1) << std::min(retry_idx, Int64(62));
    /// Overflow check: a*b <= c iff a <= c/b iff a <= floor(c/b).
    if (refresh_settings[RefreshSetting::refresh_retry_initial_backoff_ms] <= refresh_settings[RefreshSetting::refresh_retry_max_backoff_ms] / multiplier)
        delay_ms = refresh_settings[RefreshSetting::refresh_retry_initial_backoff_ms] * multiplier;
    else
        delay_ms = refresh_settings[RefreshSetting::refresh_retry_max_backoff_ms];
    return std::chrono::milliseconds(delay_ms);
}

std::tuple<std::chrono::system_clock::time_point, std::chrono::sys_seconds, RefreshTask::CoordinationZnode>
RefreshTask::determineNextRefreshTime(std::chrono::sys_seconds now)
{
    auto znode = coordination.root_znode;
    if (refresh_settings[RefreshSetting::refresh_retries] >= 0 && znode.attempt_number > refresh_settings[RefreshSetting::refresh_retries])
    {
        /// Skip to the next scheduled refresh, as if a refresh succeeded.
        znode.last_completed_timeslot = refresh_schedule.timeslotForCompletedRefresh(znode.last_completed_timeslot, znode.last_attempt_time, znode.last_attempt_time, false);
        znode.attempt_number = 0;
    }
    auto timeslot = refresh_schedule.advance(znode.last_completed_timeslot);

    std::chrono::system_clock::time_point when;
    if (znode.attempt_number == 0)
        when = refresh_schedule.addRandomSpread(timeslot, znode.randomness);
    else
        when = znode.last_attempt_time + backoff(znode.attempt_number - 1, refresh_settings);

    znode.previous_attempt_error = "";
    if (!znode.last_attempt_succeeded && znode.last_attempt_time.time_since_epoch().count() != 0)
    {
        if (znode.last_attempt_error.empty())
            znode.previous_attempt_error = fmt::format("Replica '{}' went away", znode.last_attempt_replica);
        else
            znode.previous_attempt_error = znode.last_attempt_error;
    }

    znode.attempt_number += 1;
    znode.last_attempt_time = now;
    znode.last_attempt_replica = coordination.replica_name;
    znode.last_attempt_error = "";
    znode.last_attempt_succeeded = false;

    return {when, timeslot, znode};
}

void RefreshTask::scheduleRefresh(std::lock_guard<std::mutex> &)
{
    if (state != RefreshState::Running)
        state = RefreshState::Scheduling;
    refresh_task->schedule();
}

void RefreshTask::setState(RefreshState s, std::unique_lock<std::mutex> & lock)
{
    chassert(lock.owns_lock());
    state = s;
    if (s != RefreshState::Running && s != RefreshState::Scheduling)
        refresh_cv.notify_all();
}

void RefreshTask::readZnodesIfNeeded(std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::unique_lock<std::mutex> & lock)
{
    chassert(lock.owns_lock());
    if (!coordination.coordinated || !coordination.watches->should_reread_znodes.load())
        return;

    coordination.watches->should_reread_znodes.store(false);
    auto prev_last_completed_timeslot = coordination.root_znode.last_completed_timeslot;

    lock.unlock();

    if (!zookeeper->isFeatureEnabled(KeeperFeatureFlag::MULTI_READ))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Keeper server doesn't support multi-reads. Refreshable materialized views won't work.");

    /// Set watches. (This is a lot of code, is there a better way?)
    if (!coordination.watches->root_watch_active.load())
    {
        coordination.watches->root_watch_active.store(true);
        zookeeper->existsWatch(coordination.path, nullptr,
            [w = coordination.watches, task_waker = refresh_task->getWatchCallback()](const Coordination::WatchResponse & response)
            {
                w->root_watch_active.store(false);
                w->should_reread_znodes.store(true);
                task_waker(response);
            });
    }
    if (!coordination.watches->children_watch_active.load())
    {
        coordination.watches->children_watch_active.store(true);
        zookeeper->getChildrenWatch(coordination.path, nullptr,
            [w = coordination.watches, task_waker = refresh_task->getWatchCallback()](const Coordination::WatchResponse & response)
            {
                w->children_watch_active.store(false);
                w->should_reread_znodes.store(true);
                task_waker(response);
            });
    }

    Strings paths {coordination.path, coordination.path + "/running", coordination.path + "/paused"};
    auto responses = zookeeper->tryGet(paths.begin(), paths.end());

    lock.lock();

    if (responses[0].error != Coordination::Error::ZOK)
        throw Coordination::Exception::fromPath(responses[0].error, paths[0]);
    for (size_t i = 1; i < 3; ++i)
        if (responses[i].error != Coordination::Error::ZOK && responses[i].error != Coordination::Error::ZNONODE)
            throw Coordination::Exception::fromPath(responses[i].error, paths[i]);

    coordination.root_znode.parse(responses[0].data);
    coordination.root_znode.version = responses[0].stat.version;
    coordination.running_znode_exists = responses[1].error == Coordination::Error::ZOK;
    coordination.paused_znode_exists = responses[2].error == Coordination::Error::ZOK;

    if (coordination.root_znode.last_completed_timeslot != prev_last_completed_timeslot)
    {
        lock.unlock();
        view->getContext()->getRefreshSet().notifyDependents(view->getStorageID());
        lock.lock();
    }
}

bool RefreshTask::updateCoordinationState(CoordinationZnode root, bool running, std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::unique_lock<std::mutex> & lock)
{
    chassert(lock.owns_lock());
    int32_t version = -1;
    if (coordination.coordinated)
    {
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeSetRequest(coordination.path, root.toString(), root.version));
        if (running)
            ops.emplace_back(zkutil::makeCreateRequest(coordination.path + "/running", coordination.replica_name, zkutil::CreateMode::Ephemeral));
        else
            ops.emplace_back(zkutil::makeRemoveRequest(coordination.path + "/running", -1));

        Coordination::Responses responses;

        lock.unlock();
        auto code = zookeeper->tryMulti(ops, responses);
        lock.lock();

        if (running && responses[0]->error == Coordination::Error::ZBADVERSION)
            /// Lost the race, this is normal, don't log a stack trace.
            return false;
        zkutil::KeeperMultiException::check(code, ops, responses);
        version = dynamic_cast<Coordination::SetResponse &>(*responses[0]).stat.version;

    }
    coordination.root_znode = root;
    coordination.root_znode.version = version;
    coordination.running_znode_exists = running;
    return true;
}

void RefreshTask::removeRunningZnodeIfMine(std::shared_ptr<zkutil::ZooKeeper> zookeeper)
{
    Coordination::Stat stat;
    String data;
    if (zookeeper->tryGet(coordination.path + "/running", data, &stat) && data == coordination.replica_name)
    {
        LOG_WARNING(log, "Removing unexpectedly lingering znode {}", coordination.path + "/running");
        zookeeper->tryRemove(coordination.path + "/running", stat.version);
    }
}

void RefreshTask::interruptExecution()
{
    chassert(!mutex.try_lock());
    std::unique_lock lock(execution.executor_mutex);
    if (execution.interrupt_execution.exchange(true))
        return;
    if (execution.executor)
    {
        execution.executor->cancel();
        LOG_DEBUG(log, "Cancelling refresh");
    }
}

std::tuple<StoragePtr, TableLockHolder> RefreshTask::getAndLockTargetTable(const StorageID & storage_id, const ContextPtr & context)
{
    ///  1. Get table by name.
    ///  2. Check that it's not dropped locally.
    ///     (After that, it can't be dropped during the query because we're holding StoragePtr and
    ///      TableLockHolder.)
    ///     If this fails, retry and expect to see a different table by the same name.
    ///  3. Do SYSTEM SYNC REPLICA. May fail if the table is being dropped.
    ///     If this fails, retry until we see a different table by the same name.

    StoragePtr prev_storage;
    bool prev_table_dropped_locally = false;
    std::exception_ptr exception;

    for (int attempt = 0; attempt < 10; ++attempt)
    {
        if (attempt > 0)
        {
            if (prev_table_dropped_locally)
            {
                ProfileEvents::increment(ProfileEvents::RefreshableViewLockTableRetry);
            }
            else
            {
                /// We're waiting for DatabaseReplicated to catch up and see the new table.
                ProfileEvents::increment(ProfileEvents::RefreshableViewSyncReplicaRetry);
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        StoragePtr storage = DatabaseCatalog::instance().getTable(storage_id, context);

        if (storage == prev_storage)
        {
            if (prev_table_dropped_locally)
                // Table was dropped but is still accessible in DatabaseCatalog.
                // Either ABA problem or something's broken. Don't retry.
                break;
            continue;
        }
        prev_storage = storage;

        TableLockHolder storage_lock = storage->tryLockForShare(context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
        if (!storage_lock)
        {
            prev_table_dropped_locally = true;
            continue;
        }

        if (coordination.coordinated)
        {
            std::lock_guard lock(replica_sync_mutex);
            UUID uuid = storage->getStorageID().uuid;
            if (uuid != last_synced_inner_uuid)
            {
                try
                {
                    InterpreterSystemQuery::trySyncReplica(storage, SyncReplicaMode::DEFAULT, {}, context);
                    ProfileEvents::increment(ProfileEvents::RefreshableViewSyncReplicaSuccess);
                }
                catch (Exception & e)
                {
                    if (e.code() != ErrorCodes::ABORTED)
                        throw;

                    /// Work around this race condition:
                    ///  1. Another replica does a refresh: create table X, insert, rename.
                    ///  2. This replica sees table X, but not its data yet.
                    ///  3. Another replica does another refresh: create table Y, insert, rename,
                    ///     drop table X.
                    ///  4. This replica's DatabaseReplicated shuts down table X, and the
                    ///     trySyncReplica fails with "Shutdown is called for table" exception.
                    ///     X may still not have all data. ReplicatedMergeTree shutdown stops
                    ///     data part exchange, so there's no hope of getting all data out of X.
                    /// In this case we retry table lookup in hopes of seeing the new table Y.
                    LOG_DEBUG(log, "Retrying after exception when syncing replica: {}", e.message());
                    exception = std::current_exception();
                    prev_table_dropped_locally = false;
                    continue;
                }

                /// (Race condition: this may revert from a newer uuid to an older one. This doesn't
                ///  break anything, just causes an unnecessary sync. Should be rare.)
                last_synced_inner_uuid = uuid;
            }
        }

        return {storage, storage_lock};
    }

    if (prev_table_dropped_locally)
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} is dropped or detached", storage_id.getFullNameNotQuoted());
    else
        std::rethrow_exception(exception);
}

std::chrono::system_clock::time_point RefreshTask::currentTime() const
{
    Int64 fake = scheduling.fake_clock.load(std::memory_order::relaxed);
    if (fake == INT64_MIN)
        return std::chrono::system_clock::now();
    return std::chrono::system_clock::time_point(std::chrono::seconds(fake));
}

void RefreshTask::setRefreshSetHandleUnlock(RefreshSet::Handle && set_handle_)
{
    set_handle = std::move(set_handle_);
}

void RefreshTask::CoordinationZnode::randomize()
{
    randomness = std::uniform_int_distribution(Int64(-1e9), Int64(1e9))(thread_local_rng);
}

String RefreshTask::CoordinationZnode::toString() const
{
    WriteBufferFromOwnString out;
    /// "format version" should be incremented when making incompatible change, to make older servers
    /// refuse to parse it. For backwards compatible changes, just add new fields at the end and old
    /// servers will ignore them.
    out << "format version: 1\n"
        << "last_completed_timeslot: " << Int64(last_completed_timeslot.time_since_epoch().count()) << "\n"
        << "last_success_time: " << Int64(last_success_time.time_since_epoch().count()) << "\n"
        << "last_success_duration_ms: " << Int64(last_success_duration.count()) << "\n"
        << "last_success_table_uuid: " << last_success_table_uuid << "\n"
        << "last_attempt_time: " << Int64(last_attempt_time.time_since_epoch().count()) << "\n"
        << "last_attempt_replica: " << escape << last_attempt_replica << "\n"
        << "last_attempt_error: " << escape << last_attempt_error << "\n"
        << "last_attempt_succeeded: " << last_attempt_succeeded << "\n"
        << "previous_attempt_error: " << escape << previous_attempt_error << "\n"
        << "attempt_number: " << attempt_number << "\n"
        << "randomness: " << randomness << "\n";
    return out.str();
}

void RefreshTask::CoordinationZnode::parse(const String & data)
{
    ReadBufferFromString in(data);
    Int64 last_completed_timeslot_int;
    Int64 last_success_time_int;
    Int64 last_success_duration_int;
    Int64 last_attempt_time_int;
    in >> "format version: 1\n"
       >> "last_completed_timeslot: " >> last_completed_timeslot_int >> "\n"
       >> "last_success_time: " >> last_success_time_int >> "\n"
       >> "last_success_duration_ms: " >> last_success_duration_int >> "\n"
       >> "last_success_table_uuid: " >> last_success_table_uuid >> "\n"
       >> "last_attempt_time: " >> last_attempt_time_int >> "\n"
       >> "last_attempt_replica: " >> escape >> last_attempt_replica >> "\n"
       >> "last_attempt_error: " >> escape >> last_attempt_error >> "\n"
       >> "last_attempt_succeeded: " >> last_attempt_succeeded >> "\n"
       >> "previous_attempt_error: " >> escape >> previous_attempt_error >> "\n"
       >> "attempt_number: " >> attempt_number >> "\n"
       >> "randomness: " >> randomness >> "\n";
    last_completed_timeslot = std::chrono::sys_seconds(std::chrono::seconds(last_completed_timeslot_int));
    last_success_time = std::chrono::sys_seconds(std::chrono::seconds(last_success_time_int));
    last_success_duration = std::chrono::milliseconds(last_success_duration_int);
    last_attempt_time = std::chrono::sys_seconds(std::chrono::seconds(last_attempt_time_int));
}

}
