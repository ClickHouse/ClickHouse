#include <thread>
#include <Storages/MaterializedView/RefreshTask.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Databases/DatabaseReplicated.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/queryNormalization.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Storages/StorageMaterializedView.h>
#include <base/scope_guard.h>
#include <Common/CurrentMetrics.h>
#include <Common/QueryScope.h>
#include <Common/FailPoint.h>
#include <Common/Macros.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/thread_local_rng.h>


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
    extern const Event ZooKeeperWatchTriggeredMaterializedViewRefresh;
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
    extern const int TABLE_UUID_MISMATCH;
}

RefreshTask::RefreshTask(
    StorageMaterializedView * view_, ContextPtr context, const DB::ASTRefreshStrategy & strategy, std::vector<StorageID> initial_dependencies_, bool attach, bool coordinated, bool empty, bool is_restore_from_backup)
    : view(view_)
    , refresh_schedule(strategy)
    , initial_dependencies(std::move(initial_dependencies_))
    , refresh_append(strategy.append)
{
    createLogger(view->getStorageID());

    auto component_guard = Coordination::setCurrentComponent("RefreshTask::RefreshTask");
    if (strategy.settings != nullptr)
        refresh_settings.applyChanges(strategy.settings->changes);

    coordination.root_znode.randomize();
    if (empty)
    {
        /// To skip initial refresh, set the initial scheduling-related state as if this view was just refreshed.

        coordination.root_znode.last_completed_timeslot = std::chrono::floor<std::chrono::seconds>(currentTime());

        if (coordinated || !attach)
        {
            /// We want `CREATE ... REFRESH DEPENDS ON ... EMPTY` to do first refresh after
            /// dependencies' *next* refresh after this view is created.
            AllDependenciesInfo dependencies;
            collectDependencyStatesUnlocked(dependencies, initial_dependencies);
            coordination.root_znode.last_success_dependencies = dependencies;
        }
    }
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
            if (!attach && !is_restore_from_backup)
            {
                /// (It would be possible to avoid using these features, if needed.)
                if (!zookeeper->isFeatureEnabled(KeeperFeatureFlag::MULTI_READ) ||
                    !zookeeper->isFeatureEnabled(KeeperFeatureFlag::CREATE_IF_NOT_EXISTS))
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Keeper server doesn't have all feature flags required by refreshable MV: MULTI_READ, CREATE_IF_NOT_EXISTS");
            }

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

void RefreshTask::createLogger(const StorageID & storage_id)
{
    std::lock_guard lock(logger_mutex);
    current_logger = ::getLogger(fmt::format("RefreshTask({})", storage_id.getFullTableName()));
}

LoggerPtr RefreshTask::getLogger()
{
    std::lock_guard lock(logger_mutex);
    return current_logger;
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
    std::vector<StorageID> deps;
    if (strategy.dependencies)
        for (auto && dependency : strategy.dependencies->children)
            deps.emplace_back(dependency->as<const ASTTableIdentifier &>());

    auto task = std::make_shared<RefreshTask>(view, context, strategy, std::move(deps), attach, coordinated, empty, is_restore_from_backup);

    task->scheduling_task = context->getSchedulePool().createTask(view->getStorageID(), "RefreshSched",
        [self = task.get()] { self->doScheduling(/*is_shutdown=*/ false); });
    task->execution_task = context->getSchedulePool().createTask(view->getStorageID(), "RefreshExec",
        [self = task.get()] { self->executeRefresh(); });

    task->watch_callback = std::make_shared<Coordination::WatchCallback>([w = task->coordination.watches, task_waker = task->scheduling_task->getWatchCallback()](const Coordination::WatchResponse & response)
    {
        w->root_watch_active.store(false);
        w->children_watch_active.store(false);
        w->should_reread_znodes.store(true);
        (*task_waker)(response);
    });

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
    /// Without this we can deadlock waiting for execution_task because this shutdown happens from the same DDL thread for which CREATE/EXCHANGE/DROP wait.
    execution.cancel_ddl_queries.request_stop();

    /// Wait for the tasks to return and prevent them from being scheduled in future.
    scheduling_task->deactivate();
    execution_task->deactivate();

    /// Best-effort final update of information in zookeeper, to reflect that this replica is not
    /// running a refresh anymore.
    try
    {
        doScheduling(/*is_shutdown=*/ true);
    }
    catch (...)
    {
        /// Avoid throwing from shutdown().
        /// If we failed to write to zookeeper, other replicas won't start refresh until our
        /// zookeeper session expires (+ grace period). This is not a problem if this
        /// shutdown() is caused by server shutdown or by table DROP, but may be bad if it's a DETACH
        /// (and we'll hold on to the session indefinitely).
        tryLogCurrentException(getLogger(), "Keeper error during RMV shutdown");
    }

    /// Remove from RefreshSet on DROP, without waiting for the IStorage to be destroyed.
    /// This matters because a table may get dropped and immediately created again with the same name,
    /// while the old table's IStorage still exists (pinned by ongoing queries).
    /// (Also, RefreshSet holds a shared_ptr to us.)
    ContextPtr context;
    StorageID storage_id = StorageID::createEmpty();
    {
        std::lock_guard guard(mutex);
        storage_id = set_handle.getID();
        set_handle.reset();
        if (view)
            context = view->getContext();
        view = nullptr;
    }

    /// Wake up any threads blocked in wait(), so they can see !view and throw TABLE_IS_DROPPED.
    /// Without this, wait() would block forever after deactivate() prevents the background task
    /// from running (and therefore from ever notifying wait_cv).
    wait_cv.notify_all();

    if (context)
        /// If another view DEPENDS ON this one, let it know that its dependency is missing now.
        context->getRefreshSet().notifyDependents(storage_id);
}

void RefreshTask::drop(ContextPtr context, bool is_shared_db)
{
    if (!coordination.coordinated)
        return;

    auto component_guard = Coordination::setCurrentComponent("RefreshTask::drop");
    auto zookeeper = context->getZooKeeper();

    zookeeper->tryRemove(coordination.path + "/replicas/" + coordination.replica_name);

    /// If no replicas left, remove the coordination znode.
    /// In Shared DB, let SharedDatabaseCatalog handle it.
    if (is_shared_db)
        return;

    /// If no replicas left, remove the coordination znode.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(coordination.path + "/replicas", -1));
    String paused_path = coordination.path + "/paused";
    if (zookeeper->exists(paused_path))
        ops.emplace_back(zkutil::makeRemoveRequest(paused_path, -1));
    String running_path = coordination.path + "/running";
    if (zookeeper->exists(running_path))
    {
        /// shutdown() was supposed to delete it.
        LOG_ERROR(getLogger(), "Unexpected 'running' znode when dropping refreshable materialized view.");
        ops.emplace_back(zkutil::makeRemoveRequest(running_path, -1));
    }
    ops.emplace_back(zkutil::makeRemoveRequest(coordination.path, -1));
    Coordination::Responses responses;
    auto code = zookeeper->tryMulti(ops, responses);
    if (responses[0]->error != Coordination::Error::ZNOTEMPTY && responses[0]->error != Coordination::Error::ZNONODE)
        zkutil::KeeperMultiException::check(code, ops, responses);
}

void RefreshTask::rename(StorageID new_id, StorageID new_inner_table_id)
{
    ContextPtr context;
    StorageID old_id = StorageID::createEmpty();
    {
        std::lock_guard guard(mutex);
        createLogger(new_id);
        if (set_handle)
        {
            old_id = set_handle.getID();
            set_handle.rename(new_id, refresh_append ? std::nullopt : std::make_optional(new_inner_table_id));
        }
        if (view)
            context = view->getContext();
    }
    if (context)
    {
        /// If another view DEPENDS ON the new name of this view, let it know that a view with such
        /// name now exists.
        context->getRefreshSet().notifyDependents(new_id);
        if (!old_id.empty())
            context->getRefreshSet().notifyDependents(old_id);
    }
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
    return Info {.view_id = set_handle.getID(), .state = state, .next_refresh_time = next_refresh_time, .znode = coordination.root_znode, .replica_name = coordination.replica_name, .refresh_running = coordination.root_znode.refresh_running, .progress = execution.progress.getValues(), .unexpected_error = scheduling.unexpected_error};
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
    bool was_already_stopped = std::exchange(scheduling.stop_requested, true);
    /// Always interrupt the in-flight refresh. This matters in the PAUSE-then-STOP sequence:
    /// `SYSTEM PAUSE VIEW` leaves the running refresh alone but sets `stop_requested`, and a
    /// subsequent `SYSTEM STOP VIEW` must still cancel it. `interruptExecution` is idempotent
    /// (guarded by `execution.interrupt_execution`) so repeated calls are safe.
    interruptExecution();
    if (!was_already_stopped)
        scheduleRefresh(guard);
}

void RefreshTask::pause()
{
    std::lock_guard guard(mutex);
    /// Do NOT interrupt the currently running refresh. Only prevent future refreshes.
    /// If `stop_requested` was already set (e.g. by `SYSTEM STOP VIEW`), this is a no-op.
    if (std::exchange(scheduling.stop_requested, true))
        return;
    scheduleRefresh(guard);
}

void RefreshTask::startReplicated()
{
    auto component_guard = Coordination::setCurrentComponent("RefreshTask::startReplicated");
    if (!coordination.coordinated)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Refreshable materialized view is not coordinated.");

    const auto zookeeper = [this]()
    {
        std::lock_guard guard(mutex);
        if (!view)
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "The table was dropped or detached");
        return view->getContext()->getZooKeeper();
    }();

    String path = coordination.path + "/paused";
    auto code = zookeeper->tryRemove(path);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
        throw Coordination::Exception::fromPath(code, path);
}

void RefreshTask::stopReplicated(const String & reason)
{
    if (!coordination.coordinated)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Refreshable materialized view is not coordinated.");

    auto component_guard = Coordination::setCurrentComponent("RefreshTask::stopReplicated");
    const auto zookeeper = [this]()
    {
        std::lock_guard guard(mutex);
        if (!view)
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "The table was dropped or detached");
        return view->getContext()->getZooKeeper();
    }();

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

void RefreshTask::wait(const ContextPtr & context)
{
    std::unique_lock lock(mutex);

    /// Complicated wait logic to make sure SYSTEM WAIT VIEW behaves intuitively in various cases, e.g.:
    ///  * After SYSTEM REFRESH VIEW - wait for the requested refresh (out_of_schedule_refresh_requested).
    ///     - If SYSTEM REFRESH VIEW happened when another refresh was in progress, wait for both
    ///       refreshes. (That's why there are two wait_cv.wait calls here.)
    ///  * After SYSTEM START VIEW - if it starts a refresh right away (e.g. the view was paused for
    ///    longer than refresh period), wait for that refresh.
    ///    (That's why we set state to Scheduling in start().)
    ///  * If a refresh finished (successfully or not) and another one started immediately, stop
    ///    waiting (except in the out_of_schedule_refresh_requested case per above).
    ///    (That's why we check for last_success_end_time and attempt_number change.)
    /// Perhaps this could be simplified e.g. by adding a global refresh attempt counter; but note
    /// that we'd still need two wait_cv.wait calls.

    /// If an out-of-schedule refresh was requested, wait for that requested refresh to *start*
    /// (or for the view to be stopped / shut down).
    wait_cv.wait(lock, [&]
        {
            return !scheduling.out_of_schedule_refresh_requested || !view || state == RefreshState::Disabled;
        });
    /// Wait for currently running refresh to complete.
    auto seen_success_end_time = coordination.root_znode.last_success_end_time;
    wait_cv.wait(lock, [&] {
        if (!view)
            /// Table was dropped, or server shutdown. Stop waiting.
            return true;
        if (state == RefreshState::Scheduling)
            /// We're not sure whether refresh is running. Keep waiting.
            return false;
        if (state == RefreshState::Running || state == RefreshState::RunningOnAnotherReplica)
        {
            if (coordination.root_znode.last_success_end_time > seen_success_end_time)
                /// Refresh completed, and immediately another one started.
                return true;
            /// Wait for refresh to complete.
            return false;
        }
        /// No refresh running.
        return true;
    });

    if (!view)
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "The table was dropped or detached");
    if (state == RefreshState::Disabled)
        return;
    if (!coordination.root_znode.refresh_running && !coordination.root_znode.last_attempt_succeeded && coordination.root_znode.last_attempt_time.time_since_epoch().count() != 0)
        throw Exception(ErrorCodes::REFRESH_FAILED,
            "Refresh failed{}: {}", coordination.coordinated ? " (on replica " + coordination.root_znode.last_attempt_replica + ")" : "",
            coordination.root_znode.last_attempt_error.empty() ? "Replica went away" : coordination.root_znode.last_attempt_error);
    if (coordination.root_znode.refresh_running && !coordination.root_znode.previous_attempt_error.empty())
        throw Exception(ErrorCodes::REFRESH_FAILED,
            "Refresh failed: {}", coordination.root_znode.previous_attempt_error);

    lock.unlock();

    if (coordination.coordinated && !refresh_append)
        waitForLatestTargetTable(context);
}

void RefreshTask::waitForLatestTargetTable(const ContextPtr & context)
{
    chassert(coordination.coordinated);
    chassert(!refresh_append);

    UInt64 backoff_ms = 100;
    const UInt64 max_backoff_ms = 1000;
    const int max_attempts = 10;
    auto start_time = std::chrono::steady_clock::now();

    UUID expected_table_uuid {};
    std::optional<UUID> found_table_uuid;

    for (int attempt = 0; attempt < max_attempts; ++attempt)
    {
        if (attempt > 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(backoff_ms * 2, max_backoff_ms);
        }

        std::unique_lock lock(mutex);

        if (coordination.root_znode.last_success_time.time_since_epoch().count() == 0)
            return; // no refreshes happened yet, nothing to sync
        if (!view)
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "The table was dropped or detached");

        expected_table_uuid = coordination.root_znode.last_success_table_uuid;
        StorageID storage_id = view->getTargetTableId();

        lock.unlock();

        /// (Can't use `view` here because shutdown() may unset it in parallel with us.)
        StoragePtr storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);
        if (!storage)
            continue;
        found_table_uuid = storage->getStorageID().uuid;
        if (found_table_uuid == expected_table_uuid)
            return;
    }

    throw Exception(ErrorCodes::TABLE_UUID_MISMATCH,
        "Table with UUID {} produced by latest refresh hasn't appeared on this replica for {:.3}s. {}",
        expected_table_uuid, std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - start_time).count(),
        found_table_uuid.has_value() ? fmt::format("Saw table with UUID {} instead.", *found_table_uuid) : String("There's no table at all, on this replica."));
}

bool RefreshTask::tryJoinBackgroundTask(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock lock(mutex);

    execution.cancel_ddl_queries.request_stop();

    auto duration = deadline - std::chrono::steady_clock::now();
    /// (Manually clamping to 0 because the standard library used to have (and possibly still has?)
    ///  a bug that wait_until would wait forever if the timestamp is in the past.)
    duration = std::max(duration, std::chrono::steady_clock::duration(0));
    return wait_cv.wait_for(lock, duration, [&]
        {
            return state != RefreshState::Running && state != RefreshState::Scheduling;
        });
}

RefreshTask::DependencyRefreshInfo RefreshTask::getInfoForDependentViewsLocked(const std::unique_lock<std::mutex> &) const
{
    DependencyRefreshInfo info;
    info.last_success_end_time = coordination.root_znode.last_success_end_time;
    if (refresh_schedule.kind == RefreshScheduleKind::EVERY)
        info.next_refresh_timeslot = refresh_schedule.advance(coordination.root_znode.last_completed_timeslot);
    return info;
}

RefreshTask::DependencyRefreshInfo RefreshTask::getInfoForDependentViews() const
{
    std::unique_lock lock(mutex);
    return getInfoForDependentViewsLocked(lock);
}

void RefreshTask::notify()
{
    std::lock_guard guard(mutex);
    scheduleRefresh(guard);
}

void RefreshTask::setFakeTime(std::optional<Int64> t)
{
    std::unique_lock lock(mutex);
    Int64 val = t.value_or(INT64_MIN);
    LOG_INFO(getLogger(), "Set fake time: {}", val);
    scheduling.fake_clock.store(val, std::memory_order_relaxed);
    /// Reschedule task with shorter delay if currently scheduled.
    scheduling_task->scheduleAfter(100, /*overwrite*/ true, /*only_if_scheduled*/ true);
}

void RefreshTask::doScheduling(bool is_shutdown)
{
    auto component_guard = Coordination::setCurrentComponent("RefreshTask::doScheduling");
    std::unique_lock lock(mutex);

    /// The way this function generally works is:
    ///  * Look at state in zookeeper and in memory and at current time.
    ///  * If some change is needed (e.g. write to zookeeper or start a refresh), make that change,
    ///    do scheduling_task->schedule() (to inspect the new state after the change), and return.
    ///    (We avoid making multiple changes in one iteration because that just adds more
    ///     opportunities for bugs. This function should be able to pick up from ~any state anyway.)
    ///  * If no change is needed, we setState, optionally scheduling_task->scheduleAfter, and return.
    ///  * On error, we scheduling_task->schedule/scheduleAfter and return.

    try
    {
        setState(RefreshState::Scheduling, lock);

        std::shared_ptr<zkutil::ZooKeeper> zookeeper;
        if (coordination.coordinated)
            zookeeper = view->getContext()->getZooKeeper();
        readZnodesIfNeeded(zookeeper, lock);
        chassert(lock.owns_lock());

        /// Sync 3 pieces of information about currently running refresh:
        ///  * coordination.root_znode.refresh_running
        ///  * coordination.running_znode_exists
        ///  * execution.state
        /// (Why are there as many as 3? We need an ephemeral znode to notice server crashes, a
        ///  non-ephemeral znode to tolerate brief zookeeper connection loss, and in-memory state to
        ///  communicate with the thread that executes refresh.)

        auto running_znode_missing_since = coordination.running_znode_missing_since;
        coordination.running_znode_missing_since.reset(); // reassigned below if still missing

        if (coordination.root_znode.refresh_running && coordination.root_znode.last_attempt_replica == coordination.replica_name)
        {
            /// Our replica is allegedly running a refresh.

            if (is_shutdown)
            {
                chassert(execution.state != ExecutionState::State::Running);
                if (execution.state == ExecutionState::State::Requested)
                {
                    /// execution_task was deactivated by shutdown before refresh started.
                    execution.znode.last_attempt_error = "shutdown";
                    execution.znode.refresh_running = false;
                    execution.state = ExecutionState::State::Finished;
                }
            }

            switch (execution.state)
            {
                case ExecutionState::State::None:
                {
                    LOG_WARNING(getLogger(), "RMV znode says this replica is running refresh, but it isn't. Maybe we crashed and restarted recently, and the state is left over from the crashed run?");
                    CoordinationZnode znode = coordination.root_znode;
                    znode.refresh_running = false;
                    updateCoordinationState(znode, /*running=*/ false, zookeeper, lock);
                    scheduling_task->schedule();
                    break;
                }
                case ExecutionState::State::Finished:
                {
                    /// Report refresh completion (successful or not) to the znode.
                    if (execution.znode.version == coordination.root_znode.version)
                    {
                        if (!updateCoordinationState(execution.znode, /*running=*/ false, zookeeper, lock))
                            return;
                        chassert(!coordination.root_znode.refresh_running);
                    }
                    else
                    {
                        LOG_ERROR(getLogger(), "RMV znode was updated (version {} -> {}) while refresh was running (without changing refresh_running and last_attempt_replica). This should only be possible if another server has the same replica name as me.", execution.znode.version, coordination.root_znode.version);
                        CoordinationZnode znode = coordination.root_znode;
                        znode.refresh_running = false;
                        updateCoordinationState(znode, /*running=*/ false, zookeeper, lock);
                    }

                    chassert(lock.owns_lock());
                    execution.state = ExecutionState::State::None;
                    scheduling_task->schedule();
                    break;
                }
                case ExecutionState::State::Requested:
                case ExecutionState::State::Running:
                {
                    if (!coordination.running_znode_exists)
                    {
                        LOG_WARNING(getLogger(), "Re-creating ephemeral znode '{}', presumably lost on zookeeper reconnect.", coordination.path + "/running");
                        updateCoordinationState(coordination.root_znode, /*running=*/ true, zookeeper, lock, /*only_running_znode=*/ true);
                    }

                    if (view->getContext()->getRefreshSet().refreshesStopped())
                        interruptExecution();

                    setState(RefreshState::Running, lock);
                    break;
                }
            }

            return;
        }
        else
        {
            if (execution.state != ExecutionState::State::None)
            {
                LOG_ERROR(getLogger(), "RMV refresh is running locally, but keeper says there's no running refresh on this replica. This should only be possible after keeper was unavailable for a while (over 1-2 minutes).");
                if (execution.state == ExecutionState::State::Finished)
                    execution.state = ExecutionState::State::None;
                else
                    interruptExecution();
            }

            if (coordination.root_znode.refresh_running)
            {
                /// Another replica is allegedly running a refresh.

                if (!coordination.running_znode_exists)
                {
                    /// If ephemeral znode unexpectedly disappears, wait for this long to give the
                    /// owner of the znode a chance to re-create it.
                    /// Currently hard-coded as 1.25x the keeper session timeout, but it doesn't
                    /// necessarily need to be longer than session timeout, since the grace period
                    /// starts after the session already expired.
                    UInt64 grace_period_ms = zookeeper->getSessionTimeoutMS();
                    grace_period_ms += grace_period_ms / 4;

                    std::chrono::system_clock::time_point now = currentTime();
                    if (!running_znode_missing_since.has_value())
                    {
                        LOG_INFO(getLogger(), "RMV coordination znode says refresh is running on replica '{}', but there's no corresponding ephemeral znode. Waiting for {} ms before assuming that the replica crashed.", coordination.root_znode.last_attempt_replica, grace_period_ms);
                        running_znode_missing_since = now;
                    }
                    coordination.running_znode_missing_since = running_znode_missing_since;

                    std::chrono::system_clock::time_point deadline = *coordination.running_znode_missing_since + std::chrono::milliseconds(grace_period_ms);
                    if (now >= deadline)
                    {
                        LOG_WARNING(getLogger(), "Replica '{}' appears to have crashed while executing a refresh. Clearing the lock in zookeeper to allow another replica to start a new refresh.", coordination.root_znode.last_attempt_replica);

                        CoordinationZnode znode = coordination.root_znode;
                        chassert(znode.refresh_running);
                        znode.refresh_running = false;
                        updateCoordinationState(znode, /*running=*/ false, zookeeper, lock);
                        scheduling_task->schedule();
                        return;
                    }
                    else
                    {
                        scheduling_task->scheduleAfter(std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
                    }
                }

                setState(RefreshState::RunningOnAnotherReplica, lock);
                return;
            }
            else if (coordination.running_znode_exists)
            {
                LOG_ERROR(getLogger(), "RMV coordination znode says no refresh is running, but the ephemeral 'running' znode exists. This should be impossible.");
                chassert(false);
                updateCoordinationState(coordination.root_znode, /*running=*/ false, zookeeper, lock);
                scheduling_task->schedule();
                return;
            }
        }

        if (is_shutdown)
            return; // we just needed to propagate information into zookeeper

        /// Decide when to do the next refresh.

        if (scheduling.stop_requested || coordination.paused_znode_exists || view->getContext()->getRefreshSet().refreshesStopped() || coordination.read_only)
        {
            setState(RefreshState::Disabled, lock);
            return;
        }

        AllDependenciesInfo dependencies;
        bool dependencies_ok = collectDependencyStates(dependencies, lock);
        chassert(lock.owns_lock());

        auto start_time = currentTime();
        auto [when, waiting_for_dependencies, start_znode] = determineNextRefreshTime(start_time, dependencies, lock);
        next_refresh_time = when;
        bool out_of_schedule = scheduling.out_of_schedule_refresh_requested;
        if (out_of_schedule)
        {
            chassert(start_znode.attempt_number > 0);
            start_znode.attempt_number -= 1;
        }
        else if (start_time < when || waiting_for_dependencies)
        {
            /// If we're not refreshing for two reasons at once (dependencies not satisfied,
            /// scheduled time not arrived yet), we'd like system.view_refreshes to show
            /// state Scheduled rather than WaitingForDependencies.
            if (when == std::chrono::system_clock::time_point::max() || start_time >= when || !dependencies_ok)
            {
                if (dependencies_ok)
                    setState(RefreshState::WaitingForDependencies, lock);
                else
                    setState(RefreshState::MissingDependencies, lock);
            }
            else
            {
                size_t delay_ms = std::chrono::duration_cast<std::chrono::milliseconds>(when - start_time).count();
                /// If we're in a test that fakes the clock, poll every 100ms.
                if (scheduling.fake_clock.load(std::memory_order_relaxed) != INT64_MIN)
                    delay_ms = 100;
                scheduling_task->scheduleAfter(delay_ms);
                setState(RefreshState::Scheduled, lock);
            }
            return;
        }

        /// The time to start next refresh is now!

        /// Write to keeper.
        if (!updateCoordinationState(start_znode, /*running=*/ true, zookeeper, lock))
            return;
        chassert(lock.owns_lock());

        scheduling.out_of_schedule_refresh_requested = false;

        chassert(execution.state == ExecutionState::State::None);
        execution.interrupt_execution.store(false);
        execution.znode = coordination.root_znode;
        execution.start_time = start_time;
        execution.dependencies = std::move(dependencies);
        execution.out_of_schedule = out_of_schedule;
        execution.state = ExecutionState::State::Requested;

        execution_task->schedule();
        setState(RefreshState::Running, lock);
    }
    catch (Coordination::Exception &)
    {
        tryLogCurrentException(getLogger(), "Keeper error");
        if (!lock.owns_lock())
            lock.lock();

        chassert(state == RefreshState::Scheduling);
        coordination.watches->should_reread_znodes.store(true);
        scheduling_task->scheduleAfter(5000);
    }
    catch (...)
    {
        if (!lock.owns_lock())
            lock.lock();
        scheduling.stop_requested = true;
        scheduling.unexpected_error = getCurrentExceptionMessage(true);
        coordination.watches->should_reread_znodes.store(true);
        interruptExecution();
        setState(RefreshState::Scheduling, lock);
        scheduling_task->schedule();

        tryLogCurrentException(getLogger(),
            "Unexpected exception in refresh scheduling. The view will be stopped.");
#ifdef DEBUG_OR_SANITIZER_BUILD
        abortOnFailedAssertion("Unexpected exception in refresh scheduling");
#endif
    }
}

void RefreshTask::executeRefresh()
{
    std::unique_lock lock(mutex);

    chassert(execution.state == ExecutionState::State::Requested);
    execution.state = ExecutionState::State::Running;

    Stopwatch stopwatch;
    int32_t root_znode_version = execution.znode.version;
    String error_message;
    std::optional<UUID> new_table_uuid;

    String log_comment = fmt::format("refresh of {}", view->getStorageID().getFullTableName());
    if (execution.znode.attempt_number > 1)
        log_comment += fmt::format(" (attempt {}/{})", execution.znode.attempt_number, refresh_settings[RefreshSetting::refresh_retries] + 1);

    std::vector<StorageID> deps = set_handle.getDependencies();

    lock.unlock();
    try
    {
        CurrentMetrics::Increment metric_inc(CurrentMetrics::RefreshingViews);
        new_table_uuid = executeRefreshUnlocked(root_znode_version, deps, log_comment, error_message);
    }
    catch (...)
    {
        tryLogCurrentException(getLogger(), "Unexpected exception during refresh");
        chassert(false);
        error_message = getCurrentExceptionMessage(true);
    }
    lock.lock();

    auto start_time_seconds = std::chrono::floor<std::chrono::seconds>(execution.start_time);
    auto now = currentTime();
    auto end_time_seconds = std::chrono::floor<std::chrono::seconds>(now);
    CoordinationZnode znode = execution.znode;
    znode.last_attempt_time = end_time_seconds;
    znode.last_attempt_error = error_message;
    znode.refresh_running = false;
    if (new_table_uuid.has_value())
    {
        znode.last_attempt_succeeded = true;
        znode.last_completed_timeslot = refresh_schedule.timeslotForCompletedRefresh(znode.last_completed_timeslot, start_time_seconds, end_time_seconds, execution.out_of_schedule);
        znode.last_success_time = start_time_seconds;
        znode.last_success_duration = std::chrono::milliseconds(stopwatch.elapsedMilliseconds());
        znode.last_success_table_uuid = *new_table_uuid;
        if (now > znode.last_success_end_time)
            znode.last_success_end_time = now;
        else
            /// Must monotonically increase, dependencies rely on it.
            znode.last_success_end_time += std::chrono::nanoseconds(1);
        znode.last_success_dependencies = std::move(execution.dependencies);
        znode.previous_attempt_error = "";
        znode.attempt_number = 0;
        znode.randomize();
    }
    execution.znode = znode;

    chassert(execution.state == ExecutionState::State::Running);
    execution.state = ExecutionState::State::Finished;

    scheduling_task->schedule();
}

std::optional<UUID> RefreshTask::executeRefreshUnlocked(int32_t root_znode_version, std::vector<StorageID> deps, const String & log_comment, String & out_error_message)
{
    StorageID view_storage_id = view->getStorageID();
    LOG_DEBUG(getLogger(), "Refreshing view");
    execution.progress.reset();

    static constexpr bool internal = true;

    ContextMutablePtr refresh_context = view->getContext();
    ProcessList::EntryPtr process_list_entry;
    std::optional<StorageID> table_to_drop;
    auto new_table_id = StorageID::createEmpty();

    std::optional<QueryLogElement> query_log_elem;
    boost::intrusive_ptr<ASTInsertQuery> refresh_query;
    String query_for_logging;
    UInt64 normalized_query_hash = 0;
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span = std::make_shared<OpenTelemetry::SpanHolder>("query");
    Stopwatch stopwatch;

    try
    {
        refresh_context = view->createRefreshContext(log_comment);

        syncDependenciesForRefresh(deps, refresh_context);

        if (!refresh_append)
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
            QueryScope query_scope;
            std::tie(refresh_query, query_scope) = view->prepareRefresh(refresh_append, refresh_context, table_to_drop);
            new_table_id = refresh_query->table_id;

            /// Add the query to system.processes and allow it to be killed with KILL QUERY.
            query_for_logging = refresh_query->formatForLogging(
                refresh_context->getSettingsRef()[Setting::log_queries_cut_to_length]);
            normalized_query_hash = normalizedQueryHash(query_for_logging, false);

            process_list_entry = refresh_context->getProcessList().insert(
                query_for_logging, normalized_query_hash, refresh_query.get(), refresh_context, Stopwatch{CLOCK_MONOTONIC}.getStart(), internal);

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
                currentTime(), refresh_context, query_for_logging, normalized_query_hash, refresh_query, pipeline,
                &interpreter, /*internal*/ internal, view_storage_id.database_name,
                view_storage_id.table_name, /*async_insert*/ false);

            if (!pipeline.completed())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Pipeline for view {} refresh must be completed", view_storage_id.getFullTableName());

            {
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

                /// `executor` must be destroyed before `pipeline`!
            }

            logQueryFinish(*query_log_elem, refresh_context, refresh_query, std::move(pipeline), /*pulling_pipeline=*/false, query_span, QueryResultCacheUsage::None, /*internal=*/internal);
            query_log_elem = std::nullopt;
            query_span = nullptr;
        }

        /// Exchange tables.
        if (!refresh_append)
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
            logQueryException(*query_log_elem, refresh_context, stopwatch, refresh_query, query_span, /*internal*/ internal, /*log_error*/ !cancelled);
        }
        else
        {
            /// Failed when creating new table or when swapping tables.
            logExceptionBeforeStart(query_for_logging, normalized_query_hash, refresh_context,
                                    /*ast*/ nullptr, query_span, stopwatch.elapsedMilliseconds(), /*internal*/ internal);
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

void RefreshTask::notifyDependentsIfNeeded(std::unique_lock<std::mutex> & lock)
{
    chassert(lock.owns_lock());
    auto info = getInfoForDependentViewsLocked(lock);
    if (info != coordination.notified_dependents)
    {
        coordination.notified_dependents = info;
        lock.unlock();
        view->getContext()->getRefreshSet().notifyDependents(view->getStorageID());
        lock.lock();
    }
}

bool RefreshTask::collectDependencyStates(AllDependenciesInfo & out, std::unique_lock<std::mutex> & lock)
{
    chassert(lock.owns_lock());
    AllDependenciesInfo res;
    auto deps = set_handle.getDependencies();
    if (deps.empty())
        return true;

    lock.unlock();
    bool ok = collectDependencyStatesUnlocked(out, deps);
    lock.lock();

    return ok;
}

bool RefreshTask::collectDependencyStatesUnlocked(AllDependenciesInfo & out, const std::vector<StorageID> & deps)
{
    const RefreshSet & set = view->getContext()->getRefreshSet();
    bool all_found = true;
    for (const StorageID & id : deps)
    {
        DependencyRefreshInfo info;
        auto tasks = set.findTasks(id);
        if (tasks.empty())
        {
            all_found = false;
        }
        else
        {
            info = (*tasks.begin())->getInfoForDependentViews();
        }
        info.database_and_table = id.getFullTableName();
        out.tables.push_back(std::move(info));
    }

    return all_found;
}

void RefreshTask::syncDependenciesForRefresh(const std::vector<StorageID> & deps, const ContextPtr & context)
{
    const RefreshSet & set = view->getContext()->getRefreshSet();
    for (const StorageID & id : deps)
    {
        auto tasks = set.findTasks(id);
        if (!tasks.empty())
            (*tasks.begin())->syncForDependentRefresh(context);
    }
}

static std::chrono::milliseconds backoff(Int64 retry_idx, const RefreshSettings & refresh_settings)
{
    UInt64 delay_ms = 0;
    UInt64 multiplier = UInt64(1) << std::min(retry_idx, Int64(62));
    /// Overflow check: a*b <= c iff a <= c/b iff a <= floor(c/b).
    if (refresh_settings[RefreshSetting::refresh_retry_initial_backoff_ms] <= refresh_settings[RefreshSetting::refresh_retry_max_backoff_ms] / multiplier)
        delay_ms = refresh_settings[RefreshSetting::refresh_retry_initial_backoff_ms] * multiplier;
    else
        delay_ms = refresh_settings[RefreshSetting::refresh_retry_max_backoff_ms];
    return std::chrono::milliseconds(delay_ms);
}

std::tuple<std::chrono::system_clock::time_point, bool /*waiting_for_dependencies*/, RefreshTask::CoordinationZnode>
RefreshTask::determineNextRefreshTime(std::chrono::system_clock::time_point now, const AllDependenciesInfo & dependencies, const std::unique_lock<std::mutex> & lock)
{
    chassert(lock.owns_lock());
    auto znode = coordination.root_znode;
    if (refresh_settings[RefreshSetting::refresh_retries] >= 0 && znode.attempt_number > refresh_settings[RefreshSetting::refresh_retries])
    {
        /// Skip to the next scheduled refresh, as if a refresh succeeded.
        znode.last_completed_timeslot = refresh_schedule.timeslotForCompletedRefresh(znode.last_completed_timeslot, znode.last_attempt_time, znode.last_attempt_time, false);
        znode.attempt_number = 0;
    }

    std::chrono::system_clock::time_point when = std::chrono::system_clock::time_point::max();
    bool waiting_for_dependencies = false;
    if (znode.attempt_number != 0)
    {
        /// Retrying refresh. Ignore schedule and dependencies.
        when = znode.last_attempt_time + backoff(znode.attempt_number - 1, refresh_settings);
    }
    else if (dependencies.tables.empty())
    {
        /// No dependencies, use schedule.
        when = refresh_schedule.advance(znode.last_completed_timeslot);
    }
    else
    {
        std::unordered_map<String, std::chrono::sys_time<std::chrono::nanoseconds>> last_refresh_deps;
        for (const DependencyRefreshInfo & info : znode.last_success_dependencies.tables)
            last_refresh_deps[info.database_and_table] = info.last_success_end_time;

        /// Trigger refresh if all dependencies refreshed at least once since our last refresh.
        bool all_advanced = true;
        bool all_kind_every = true;
        std::chrono::sys_time<std::chrono::nanoseconds> max_time {};
        auto min_next_refresh_timeslot = std::chrono::sys_seconds::max();
        for (const DependencyRefreshInfo & info : dependencies.tables)
        {
            if (info.next_refresh_timeslot.has_value())
                min_next_refresh_timeslot = std::min(min_next_refresh_timeslot, *info.next_refresh_timeslot);
            else
                all_kind_every = false;

            auto threshold = last_refresh_deps[info.database_and_table];
            auto current = info.last_success_end_time;
            if (current > threshold)
            {
                /// The dependency has refreshed since our last refresh.
                auto & seen = scheduling.seen_dep_refresh_times[info.database_and_table];
                if (seen > threshold)
                    /// If the dependency refreshed more than once, use the oldest of these refresh times,
                    /// to avoid waiting for REFRESH AFTER indefinitely if dependency keeps refreshing.
                    current = seen;
                else
                    seen = current;
                max_time = std::max(max_time, seen);
            }
            else
            {
                all_advanced = false;
            }
        }

        /// Dependency semantics are awkward. Two different behaviors:
        ///  1. If both dependency and dependent use REFRESH EVERY, align their timeslots.
        ///     Start refresh for a given timeslot only after all dependencies moved past that timeslot.
        ///  2. Otherwise start refresh if all dependencies refreshed at least once since our last refresh.
        if (refresh_schedule.kind == RefreshScheduleKind::EVERY)
        {
            auto timeslot = refresh_schedule.advance(znode.last_completed_timeslot);
            when = timeslot;
            if (all_kind_every)
                waiting_for_dependencies = min_next_refresh_timeslot <= timeslot;
            else
                waiting_for_dependencies = !all_advanced;
        }
        else
        {
            if (all_advanced)
            {
                if (refresh_schedule.period.maxSeconds() == 0)
                    /// (Unnecessary pedantic special case: use `now` instead of `max_time` to
                    ///  refresh right away even if `max_time` is in the future because of clock skew.)
                    when = now;
                else
                    when = refresh_schedule.period.advance(std::chrono::floor<std::chrono::system_clock::duration>(max_time));
            }
        }
    }

    if (when == std::chrono::system_clock::time_point::max())
        waiting_for_dependencies = true;
    else
        when = refresh_schedule.addRandomSpread(when, znode.randomness);

    znode.previous_attempt_error = "";
    if (!znode.last_attempt_succeeded && znode.last_attempt_time.time_since_epoch().count() != 0)
    {
        if (znode.last_attempt_error.empty())
            znode.previous_attempt_error = fmt::format("Replica '{}' went away", znode.last_attempt_replica);
        else
            znode.previous_attempt_error = znode.last_attempt_error;
    }

    znode.attempt_number += 1;
    znode.last_attempt_time = std::chrono::floor<std::chrono::seconds>(now);
    znode.last_attempt_replica = coordination.replica_name;
    znode.last_attempt_error = "";
    znode.last_attempt_succeeded = false;
    znode.refresh_running = true;

    return {when, waiting_for_dependencies, znode};
}

void RefreshTask::scheduleRefresh(std::lock_guard<std::mutex> &)
{
    if (state != RefreshState::Running)
        state = RefreshState::Scheduling;
    scheduling_task->schedule();
}

void RefreshTask::setState(RefreshState s, std::unique_lock<std::mutex> & lock)
{
    chassert(lock.owns_lock());
    state = s;
    if (s != RefreshState::Scheduling)
        wait_cv.notify_all();
}

void RefreshTask::readZnodesIfNeeded(std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::unique_lock<std::mutex> & lock)
{
    chassert(lock.owns_lock());
    if (!coordination.coordinated || !coordination.watches->should_reread_znodes.load())
        return;

    coordination.watches->should_reread_znodes.store(false);

    lock.unlock();

    if (!zookeeper->isFeatureEnabled(KeeperFeatureFlag::MULTI_READ))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Keeper server doesn't support multi-reads. Refreshable materialized views won't work.");

    /// Set watches. (This is a lot of code, is there a better way?)
    Coordination::WatchCallbackPtrOrEventPtr labelled_watch{
        watch_callback, ProfileEvents::ZooKeeperWatchTriggeredMaterializedViewRefresh};
    if (!coordination.watches->root_watch_active.load())
    {
        coordination.watches->root_watch_active.store(true);
        zookeeper->existsWatch(coordination.path, nullptr, labelled_watch);
    }
    if (!coordination.watches->children_watch_active.load())
    {
        coordination.watches->children_watch_active.store(true);
        zookeeper->getChildrenWatch(coordination.path, nullptr, labelled_watch);
    }

    Strings paths {coordination.path, coordination.path + "/running", coordination.path + "/paused"};
    auto responses = zookeeper->tryGet(paths.begin(), paths.end());

    if (responses[0].error != Coordination::Error::ZOK)
        throw Coordination::Exception::fromPath(responses[0].error, paths[0]);
    for (size_t i = 1; i < 3; ++i)
        if (responses[i].error != Coordination::Error::ZOK && responses[i].error != Coordination::Error::ZNONODE)
            throw Coordination::Exception::fromPath(responses[i].error, paths[i]);

    bool running_znode_exists = responses[1].error == Coordination::Error::ZOK;
    CoordinationZnode znode;
    znode.parse(responses[0].data, running_znode_exists, getLogger());

    lock.lock();

    coordination.root_znode = znode;
    coordination.root_znode.version = responses[0].stat.version;
    coordination.running_znode_exists = running_znode_exists;
    coordination.paused_znode_exists = responses[2].error == Coordination::Error::ZOK;

    notifyDependentsIfNeeded(lock);
    wait_cv.notify_all();
}

bool RefreshTask::updateCoordinationState(CoordinationZnode root, bool running, std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::unique_lock<std::mutex> & lock, bool only_running_znode)
{
    chassert(lock.owns_lock());
    int32_t version = -1;
    if (coordination.coordinated)
    {
        Coordination::Requests ops;
        if (only_running_znode)
            ops.emplace_back(zkutil::makeCheckRequest(coordination.path, root.version));
        else
            ops.emplace_back(zkutil::makeSetRequest(coordination.path, root.toString(), root.version));
        if (running)
        {
            ops.emplace_back(
                zkutil::makeCreateRequest(coordination.path + "/running", coordination.replica_name, zkutil::CreateMode::Ephemeral, /*ignore_if_exists=*/ true));
        }
        else
        {
            /// (Avoid `try_remove = true` because it requires a keeper feature flag TRY_REMOVE that we're otherwise not using.)
            if (coordination.running_znode_exists)
                ops.emplace_back(zkutil::makeRemoveRequest(coordination.path + "/running", -1));
        }

        Coordination::Responses responses;

        lock.unlock();
        auto code = zookeeper->tryMulti(ops, responses);
        lock.lock();

        if (running && responses[0]->error == Coordination::Error::ZBADVERSION)
        {
            /// Lost the race, this is normal, don't log a stack trace.
            /// Trigger a re-read of znodes just in case, though it shouldn't be necessary because of watches.
            /// (Can we get into a situation where such re-reads keep returning stale data, and
            ///  write attempts keep failing with version mismatch, and we keep needlessly
            ///  busy-waiting and DOSing keeper?
            ///  Based on how keeper server works, this shouldn't happen: keeper provides
            ///  read-after-write consistency within a session even for failed writes. The next read
            ///  should always see the newer znode version that caused the conflict. Idk whether
            ///  this is the case in vanilla zookeeper as well.)
            coordination.watches->should_reread_znodes.store(true);
            scheduling_task->schedule();
            return false;
        }
        zkutil::KeeperMultiException::check(code, ops, responses);
        if (only_running_znode)
            version = root.version;
        else
            version = dynamic_cast<Coordination::SetResponse &>(*responses[0]).stat.version;
    }
    coordination.root_znode = root;
    coordination.root_znode.version = version;
    coordination.running_znode_exists = running;

    notifyDependentsIfNeeded(lock);
    wait_cv.notify_all();

    return true;
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
        LOG_DEBUG(getLogger(), "Cancelling refresh in {}", set_handle.getID().getFullNameNotQuoted());
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

    UInt64 backoff_ms = 100;
    const UInt64 max_backoff_ms = 1000;
    const int max_attempts = 10;

    for (int attempt = 0; attempt < max_attempts; ++attempt)
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
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
                backoff_ms = std::min(backoff_ms * 2, max_backoff_ms);
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
            std::lock_guard sync_lock(replica_sync_mutex);
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
                    LOG_DEBUG(getLogger(), "Retrying after exception when syncing replica: {}", e.message());
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

void RefreshTask::syncForDependentRefresh(const ContextPtr & context)
{
    if (!coordination.coordinated)
        return;

    if (refresh_append)
    {
        /// Do a SYNC REPLICA to make sure dependent refresh sees the rows appended by the latest dependency refresh.

        std::unique_lock lock(mutex);

        if (!view)
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Dependency's table was dropped or detached");

        auto last_success_end_time = coordination.root_znode.last_success_end_time;
        StorageID storage_id = view->getTargetTableId();

        lock.unlock();

        if (last_success_end_time.time_since_epoch().count() == 0)
            return; // never refreshed

        StoragePtr storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);
        if (!storage)
            return;

        std::lock_guard sync_lock(replica_sync_mutex);

        if (last_success_end_time == last_synced_refresh_end_time)
            /// There was no refresh since last sync. Don't sync again.
            /// Useful if there are many dependent views.
            return;

        InterpreterSystemQuery::trySyncReplica(storage, SyncReplicaMode::DEFAULT, {}, context);
        ProfileEvents::increment(ProfileEvents::RefreshableViewSyncReplicaSuccess);

        last_synced_refresh_end_time = last_success_end_time;
    }
    else
    {
        /// Wait until we see the table produced by the latest refresh.
        /// No need to SYNC REPLICA because getAndLockTargetTable will do that, as part of SELECT.
        /// (Alternatively, we could do waitForLatestTargetTable() in getAndLockTargetTable. But it
        ///  doesn't seem useful there for anything other than DEPENDS ON, because the
        ///  `root_znode.last_success_table_uuid` may itself be stale. For dependent refresh we know
        ///  it's not stale because the refresh is triggered based on the same `root_znode` contents.)
        waitForLatestTargetTable(context);
    }
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

void RefreshTask::AllDependenciesInfo::writeText(WriteBuffer & out) const
{
    static FormatSettings format_settings;

    /// One line, formatted like this (subset of json):
    /// {"foo.bar": {"last_success_end_time_ns": 123456, "unrecognized_ignored_field": 42, "unrecognized_ignored_quoted_string": "cat says: \"meow\"!"}, "xx.yy": {"last_success_end_time_ns": 654321}}
    /// For compatibility, only string and int fields can be added, see AllDependenciesInfo::readText.
    out << "{";
    bool first = true;
    for (const DependencyRefreshInfo & t : tables)
    {
        if (!first)
            out << ", ";
        first = false;
        writeJSONString(t.database_and_table, out, format_settings);
        out << ": {\"last_success_end_time_ns\": " << Int64(t.last_success_end_time.time_since_epoch().count()) << "}";
    }
    out << "}";
}

void RefreshTask::AllDependenciesInfo::readText(ReadBuffer & in)
{
    static FormatSettings::JSON json_settings;

    /// Don't be picky about whitespace, in case someone edits the znode contents by hand and assumes it's just json.
    /// (Maybe we should use a full json parser at this point.)
    skipWhitespaceIfAny(in, /*one_line=*/ true);
    in >> "{";
    skipWhitespaceIfAny(in, /*one_line=*/ true);
    while (!checkChar('}', in))
    {
        /// {"foo.bar": {
        DependencyRefreshInfo t;
        readJSONString(t.database_and_table, in, json_settings);
        skipWhitespaceIfAny(in, /*one_line=*/ true);
        in >> ":";
        skipWhitespaceIfAny(in, /*one_line=*/ true);
        in >> "{";

        while (!checkChar('}', in))
        {
            /// "field_name":
            String field_name;
            readJSONString(field_name, in, json_settings);
            skipWhitespaceIfAny(in, /*one_line=*/ true);
            in >> ":";
            skipWhitespaceIfAny(in, /*one_line=*/ true);

            /// Unquoted int or quoted json string.
            Int64 int_val = 0;
            String string_val;
            char c = 0;
            if (in.peek(c) && c != '"')
                in >> int_val;
            else
                readJSONString(string_val, in, json_settings);

            skipWhitespaceIfAny(in, /*one_line=*/ true);
            checkChar(',', in);
            skipWhitespaceIfAny(in, /*one_line=*/ true);

            if (field_name == "last_success_end_time_ns")
                t.last_success_end_time = std::chrono::sys_time<std::chrono::nanoseconds>(std::chrono::nanoseconds(int_val));
        }

        skipWhitespaceIfAny(in, /*one_line=*/ true);
        checkChar(',', in);
        skipWhitespaceIfAny(in, /*one_line=*/ true);

        tables.push_back(std::move(t));
    }
    skipWhitespaceIfAny(in, /*one_line=*/ true);
}

void RefreshTask::CoordinationZnode::randomize()
{
    randomness = std::uniform_int_distribution<Int64>(Int64(-1e9), Int64(1e9))(thread_local_rng);
}

String RefreshTask::CoordinationZnode::toString() const
{
    /// "format version" should be incremented when making incompatible change, to make older
    /// servers refuse to parse it. We should probably never do that.
    ///
    /// For backwards compatible changes, just add new fields at the end (!), and old servers will
    /// ignore them.
    ///
    /// Removing a field is complicated enough that maybe we should never do it. Procedure would be:
    ///  1. Make the field optional in `parse` but keep writing it here.
    ///  2. Update all servers and make sure they update all RMV znodes (e.g. do a refresh).
    ///  3. Stop writing the field here but keep recognizing (and ignoring) it in `parse`.
    ///  4. Update all servers etc.
    ///  5. Remove the field from `parse`.

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
        << "randomness: " << randomness << "\n"
        << "refresh_running: " << refresh_running << "\n"
        << "last_success_end_time_ns: " << Int64(last_success_end_time.time_since_epoch().count()) << "\n";

    out << "last_success_dependencies: ";
    last_success_dependencies.writeText(out);
    out << "\n";

    return out.str();
}

void RefreshTask::CoordinationZnode::parse(const String & data, bool running_znode_exists, const LoggerPtr & log_)
{
    *this = {};

    ReadBufferFromString in(data);

    String next_field_name;
    auto advance_to_next_field = [&]
    {
        next_field_name.clear();
        if (in.eof())
            return;
        assertChar('\n', in);
        if (in.eof())
            return;
        readStringUntilColon(next_field_name, in);
        assertString(": ", in);
    };
    auto optional_field = [&](const char * name, auto & out) -> bool
    {
        using T = std::remove_reference_t<decltype(out)>;

        if (next_field_name != name)
            return false;

        if constexpr (std::is_same_v<T, std::string>)
        {
            in >> escape >> out;
        }
        else if constexpr (std::is_same_v<T, std::chrono::sys_seconds>)
        {
            Int64 v = 0;
            in >> v;
            out = std::chrono::sys_seconds(std::chrono::seconds(v));
        }
        else if constexpr (std::is_same_v<T, std::chrono::milliseconds>)
        {
            Int64 v = 0;
            in >> v;
            out = std::chrono::milliseconds(v);
        }
        else if constexpr (std::is_same_v<T, std::chrono::sys_time<std::chrono::nanoseconds>>)
        {
            Int64 v = 0;
            in >> v;
            out = std::chrono::sys_time<std::chrono::nanoseconds>(std::chrono::nanoseconds(v));
        }
        else if constexpr (std::is_same_v<T, AllDependenciesInfo>)
        {
            out.readText(in);
        }
        else
        {
            in >> out;
        }

        advance_to_next_field();
        return true;
    };

    auto required_field = [&](const char * name, auto & out)
    {
        if (!optional_field(name, out))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "RMV coordination znode fields are missing or reordered: not found field '{}'", name);
    };

    in >> "format version: 1";
    advance_to_next_field();

    required_field("last_completed_timeslot", last_completed_timeslot);
    required_field("last_success_time", last_success_time);
    required_field("last_success_duration_ms", last_success_duration);
    required_field("last_success_table_uuid", last_success_table_uuid);
    required_field("last_attempt_time", last_attempt_time);
    required_field("last_attempt_replica", last_attempt_replica);
    required_field("last_attempt_error", last_attempt_error);
    required_field("last_attempt_succeeded", last_attempt_succeeded);
    required_field("previous_attempt_error", previous_attempt_error);
    required_field("attempt_number", attempt_number);
    required_field("randomness", randomness);

    refresh_running = running_znode_exists;
    optional_field("refresh_running", refresh_running);

    optional_field("last_success_end_time_ns", last_success_end_time);
    optional_field("last_success_dependencies", last_success_dependencies);

    if (!next_field_name.empty())
    {
        LOG_INFO(log_, "Unrecognized field '{}' in RMV coordination znode. Maybe the znode was written by a newer version of the server that added this field, or maybe parsing is broken.", next_field_name);
    }
}

}
