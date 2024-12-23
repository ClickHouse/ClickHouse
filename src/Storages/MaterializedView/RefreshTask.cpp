#include <Storages/MaterializedView/RefreshTask.h>

#include <Common/CurrentMetrics.h>
#include <Core/Settings.h>
#include <Common/Macros.h>
#include <Common/thread_local_rng.h>
#include <Core/ServerSettings.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/ProcessList.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Parsers/ASTCreateQuery.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Storages/StorageMaterializedView.h>

namespace CurrentMetrics
{
    extern const Metric RefreshingViews;
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
}

RefreshTask::RefreshTask(
    StorageMaterializedView * view_, ContextPtr context, const DB::ASTRefreshStrategy & strategy, bool attach, bool coordinated, bool empty)
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
        if (attach)
        {
            /// Check that this replica is registered in keeper.
            if (!zookeeper->exists(replica_path))
            {
                LOG_ERROR(log, "Attaching refreshable materialized view {} as read-only because znode {} is missing", view->getStorageID().getFullTableName(), replica_path);
                coordination.read_only = true;
            }
        }
        else
        {
            zookeeper->createAncestors(coordination.path);
            /// Create coordination znodes if they don't exist. Register this replica, throwing if already exists.
            Coordination::Requests ops;
            ops.emplace_back(zkutil::makeCreateRequest(coordination.path, coordination.root_znode.toString(), zkutil::CreateMode::Persistent, true));
            ops.emplace_back(zkutil::makeCreateRequest(coordination.path + "/replicas", "", zkutil::CreateMode::Persistent, true));
            ops.emplace_back(zkutil::makeCreateRequest(replica_path, "", zkutil::CreateMode::Persistent));
            zookeeper->multi(ops);
        }
    }
}

OwnedRefreshTask RefreshTask::create(
    StorageMaterializedView * view,
    ContextMutablePtr context,
    const DB::ASTRefreshStrategy & strategy,
    bool attach,
    bool coordinated,
    bool empty)
{
    auto task = std::make_shared<RefreshTask>(view, context, strategy, attach, coordinated, empty);

    task->refresh_task = context->getSchedulePool().createTask("RefreshTask",
        [self = task.get()] { self->refreshTask(); });

    if (strategy.dependencies)
        for (auto && dependency : strategy.dependencies->children)
            task->initial_dependencies.emplace_back(dependency->as<const ASTTableIdentifier &>());

    return OwnedRefreshTask(task);
}

void RefreshTask::startup()
{
    if (view->getContext()->getSettingsRef()[Setting::stop_refreshable_materialized_views_on_startup])
        scheduling.stop_requested = true;
    auto inner_table_id = refresh_append ? std::nullopt : std::make_optional(view->getTargetTableId());
    view->getContext()->getRefreshSet().emplace(view->getStorageID(), inner_table_id, initial_dependencies, shared_from_this());
    refresh_task->schedule();
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
    {
        std::lock_guard guard(mutex);

        refresh_schedule = RefreshSchedule(new_strategy);
        std::vector<StorageID> deps;
        if (new_strategy.dependencies)
            for (auto && dependency : new_strategy.dependencies->children)
                deps.emplace_back(dependency->as<const ASTTableIdentifier &>());

        /// Update dependency graph.
        set_handle.changeDependencies(deps);

        refresh_task->schedule();
        scheduling.dependencies_satisfied_until = std::chrono::sys_seconds(std::chrono::seconds(-1));

        refresh_settings = {};
        if (new_strategy.settings != nullptr)
            refresh_settings.applyChanges(new_strategy.settings->changes);
    }
    /// In case refresh period changed.
    view->getContext()->getRefreshSet().notifyDependents(view->getStorageID());
}

RefreshTask::Info RefreshTask::getInfo() const
{
    std::lock_guard guard(mutex);
    return Info {.view_id = set_handle.getID(), .state = state, .next_refresh_time = next_refresh_time, .znode = coordination.root_znode, .refresh_running = coordination.running_znode_exists, .progress = execution.progress.getValues()};
}

void RefreshTask::start()
{
    std::lock_guard guard(mutex);
    if (!std::exchange(scheduling.stop_requested, false))
        return;
    refresh_task->schedule();
}

void RefreshTask::stop()
{
    std::lock_guard guard(mutex);
    if (std::exchange(scheduling.stop_requested, true))
        return;
    interruptExecution();
    refresh_task->schedule();
}

void RefreshTask::run()
{
    std::lock_guard guard(mutex);
    if (std::exchange(scheduling.out_of_schedule_refresh_requested, true))
        return;
    refresh_task->schedule();
}

void RefreshTask::cancel()
{
    std::lock_guard guard(mutex);
    interruptExecution();
    refresh_task->schedule();
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
            state != RefreshState::RunningOnAnotherReplica && !scheduling.out_of_schedule_refresh_requested;
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
    refresh_task->schedule();
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
#ifdef ABORT_ON_LOGICAL_ERROR
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

            if (scheduling.stop_requested || view->getContext()->getRefreshSet().refreshesStopped() || coordination.read_only)
            {
                /// Exit the task and wait for the user to start or resume, which will schedule the task again.
                setState(RefreshState::Disabled, lock);
                break;
            }

            /// Check if it's time to refresh.
            auto now = currentTime();
            auto start_time = std::chrono::floor<std::chrono::seconds>(now);
            auto start_time_steady = std::chrono::steady_clock::now();
            auto [when, timeslot, start_znode] = determineNextRefreshTime(start_time);
            next_refresh_time = when;
            bool out_of_schedule = scheduling.out_of_schedule_refresh_requested;
            if (out_of_schedule)
            {
                chassert(start_znode.attempt_number > 0);
                start_znode.attempt_number -= 1;
            }
            else if (now < when)
            {
                size_t delay_ms = std::chrono::duration_cast<std::chrono::milliseconds>(when - now).count();
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

            lock.unlock();

            bool refreshed = false;
            String error_message;
            UUID new_table_uuid;

            try
            {
                new_table_uuid = executeRefreshUnlocked(append, root_znode_version);
                refreshed = true;
            }
            catch (...)
            {
                if (execution.interrupt_execution.load())
                {
                    error_message = "cancelled";
                    LOG_INFO(log, "{}: Refresh cancelled", view->getStorageID().getFullTableName());
                }
                else
                {
                    error_message = getCurrentExceptionMessage(true);
                    LOG_ERROR(log, "{}: Refresh failed (attempt {}/{}): {}", view->getStorageID().getFullTableName(), start_znode.attempt_number, refresh_settings[RefreshSetting::refresh_retries] + 1, error_message);
                }
            }

            lock.lock();

            setState(RefreshState::Scheduling, lock);

            auto end_time = std::chrono::floor<std::chrono::seconds>(currentTime());
            auto znode = coordination.root_znode;
            znode.last_attempt_time = end_time;
            if (refreshed)
            {
                znode.last_attempt_succeeded = true;
                znode.last_completed_timeslot = refresh_schedule.timeslotForCompletedRefresh(znode.last_completed_timeslot, start_time, end_time, out_of_schedule);
                znode.last_success_time = start_time;
                znode.last_success_duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time_steady);
                znode.last_success_table_uuid = new_table_uuid;
                znode.previous_attempt_error = "";
                znode.attempt_number = 0;
                znode.randomize();
            }
            else
            {
                znode.last_attempt_error = error_message;
            }

            bool ok = updateCoordinationState(znode, false, zookeeper, lock);
            chassert(ok);
            chassert(lock.owns_lock());

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
        coordination.watches->should_reread_znodes.store(true);
        coordination.running_znode_exists = false;
        lock.unlock();

        tryLogCurrentException(log,
            "Unexpected exception in refresh scheduling, please investigate. The view will be stopped.");
#ifdef DEBUG_OR_SANITIZER_BUILD
        abortOnFailedAssertion("Unexpected exception in refresh scheduling");
#else
        if (coordination.coordinated)
            removeRunningZnodeIfMine(view->getContext()->getZooKeeper());
#endif
    }
}

UUID RefreshTask::executeRefreshUnlocked(bool append, int32_t root_znode_version)
{
    LOG_DEBUG(log, "Refreshing view {}", view->getStorageID().getFullTableName());
    execution.progress.reset();

    ContextMutablePtr refresh_context = view->createRefreshContext();

    if (!append)
    {
        refresh_context->setParentTable(view->getStorageID().uuid);
        refresh_context->setDDLQueryCancellation(execution.cancel_ddl_queries.get_token());
        if (root_znode_version != -1)
            refresh_context->setDDLAdditionalChecksOnEnqueue({zkutil::makeCheckRequest(coordination.path, root_znode_version)});
    }

    std::optional<StorageID> table_to_drop;
    auto new_table_id = StorageID::createEmpty();
    try
    {
        {
            /// Create a table.
            auto [refresh_query, query_scope] = view->prepareRefresh(append, refresh_context, table_to_drop);
            new_table_id = refresh_query->table_id;

            /// Add the query to system.processes and allow it to be killed with KILL QUERY.
            String query_for_logging = refresh_query->formatForLogging(
                refresh_context->getSettingsRef()[Setting::log_queries_cut_to_length]);
            auto process_list_entry = refresh_context->getProcessList().insert(
                query_for_logging, refresh_query.get(), refresh_context, Stopwatch{CLOCK_MONOTONIC}.getStart());
            refresh_context->setProcessListElement(process_list_entry->getQueryStatus());
            refresh_context->setProgressCallback([this](const Progress & prog)
            {
                execution.progress.incrementPiecewiseAtomically(prog);
            });

            /// Run the query.

            BlockIO block_io = InterpreterInsertQuery(
                refresh_query,
                refresh_context,
                /* allow_materialized */ false,
                /* no_squash */ false,
                /* no_destination */ false,
                /* async_isnert */ false).execute();
            QueryPipeline & pipeline = block_io.pipeline;

            if (!pipeline.completed())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for view refresh must be completed");

            PipelineExecutor executor(pipeline.processors, pipeline.process_list_element);
            executor.setReadProgressCallback(pipeline.getReadProgressCallback());

            {
                std::unique_lock exec_lock(execution.executor_mutex);
                if (execution.interrupt_execution.load())
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh cancelled");
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
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh cancelled");
        }

        /// Exchange tables.
        if (!append)
            table_to_drop = view->exchangeTargetTable(new_table_id, refresh_context);
    }
    catch (...)
    {
        if (table_to_drop.has_value())
            view->dropTempTable(table_to_drop.value(), refresh_context);
        throw;
    }

    if (table_to_drop.has_value())
        view->dropTempTable(table_to_drop.value(), refresh_context);

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

    Strings paths {coordination.path, coordination.path + "/running"};
    auto responses = zookeeper->tryGet(paths.begin(), paths.end());

    lock.lock();

    if (responses[0].error != Coordination::Error::ZOK)
        throw Coordination::Exception::fromPath(responses[0].error, paths[0]);
    if (responses[1].error != Coordination::Error::ZOK && responses[1].error != Coordination::Error::ZNONODE)
        throw Coordination::Exception::fromPath(responses[1].error, paths[1]);

    coordination.root_znode.parse(responses[0].data);
    coordination.root_znode.version = responses[0].stat.version;
    coordination.running_znode_exists = responses[1].error == Coordination::Error::ZOK;

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
    StoragePtr storage;
    TableLockHolder storage_lock;
    for (int attempt = 0; attempt < 10; ++attempt)
    {
        StoragePtr prev_storage = std::move(storage);
        storage = DatabaseCatalog::instance().getTable(storage_id, context);
        if (storage == prev_storage)
        {
            // Table was dropped but is still accessible in DatabaseCatalog.
            // Either ABA problem or something's broken. Don't retry, just in case.
            break;
        }
        storage_lock = storage->tryLockForShare(context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
        if (storage_lock)
            break;
    }
    if (!storage_lock)
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table {} is dropped or detached", storage_id.getFullNameNotQuoted());

    if (coordination.coordinated)
    {
        UUID uuid = storage->getStorageID().uuid;

        std::lock_guard lock(replica_sync_mutex);
        if (uuid != last_synced_inner_uuid)
        {
            InterpreterSystemQuery::trySyncReplica(storage, SyncReplicaMode::DEFAULT, {}, context);

            /// (Race condition: this may revert from a newer uuid to an older one. This doesn't break
            ///  anything, just causes an unnecessary sync. Should be rare.)
            last_synced_inner_uuid = uuid;
        }
    }

    return {storage, storage_lock};
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
    randomness = std::uniform_int_distribution(Int64(-1e-9), Int64(1e9))(thread_local_rng);
}

String RefreshTask::CoordinationZnode::toString() const
{
    WriteBufferFromOwnString out;
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
    Int64 last_completed_timeslot_int, last_success_time_int, last_success_duration_int, last_attempt_time_int;
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
