#pragma once

#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MaterializedView/RefreshSchedule.h>
#include <Storages/MaterializedView/RefreshSettings.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/StopToken.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <IO/Progress.h>

#include <random>


namespace zkutil
{
    class ZooKeeper;
}

namespace DB
{

class PipelineExecutor;

class StorageMaterializedView;
class ASTRefreshStrategy;
struct OwnedRefreshTask;

enum class RefreshState
{
    Disabled = 0,
    Scheduling,
    Scheduled,
    WaitingForDependencies,
    MissingDependencies,
    Running,
    RunningOnAnotherReplica,
};

class RefreshTask : public std::enable_shared_from_this<RefreshTask>
{
public:
    struct DependencyRefreshInfo
    {
        String database_and_table;

        /// Used for detecting whether the dependency did a refresh since the last time we checked.
        std::chrono::sys_time<std::chrono::nanoseconds> last_success_end_time {};

        /// Not serialized.
        /// Used when both the dependency and the dependent have REFRESH EVERY, for matching their timeslots.
        std::optional<std::chrono::sys_seconds> next_refresh_timeslot;

        bool operator==(const DependencyRefreshInfo & rhs) const = default;
        bool operator!=(const DependencyRefreshInfo & rhs) const = default;
    };

    struct AllDependenciesInfo
    {
        std::vector<DependencyRefreshInfo> tables;

        void writeText(WriteBuffer & out) const;
        void readText(ReadBuffer & in);
    };

    /// Information stored in zookeeper shared among replicas to make sure only one replica does a
    /// refresh for each timeslot.
    /// If coordination is disabled, we still do "coordination" the same way (as if there's only one
    /// replica), but keep the CoordinationZnode in memory without writing to zookeeper.
    struct CoordinationZnode
    {
        /// "Official" time of the latest successful refresh, i.e. time according to schedule rather than wall clock,
        /// and without randomization. E.g. for REFRESH EVERY 1 DAY this timestamp is always the first second of a
        /// calendar day. 0 if no successful refresh happened yet.
        /// (We store last rather than next timeslot because it behaves better when ALTER MODIFY REFRESH reduces refresh period.)
        std::chrono::sys_seconds last_completed_timeslot;

        /// Time when the latest successful refresh started.
        std::chrono::sys_seconds last_success_time;
        std::chrono::milliseconds last_success_duration{0};
        /// Note that this may not match the DB if a refresh managed to EXCHANGE tables, then failed to write to keeper.
        /// That can only happen if last_attempt_succeeded = false.
        UUID last_success_table_uuid;

        /// Information about the last started attempt. (I.e. current attempt if a refresh is running, previous attempt if not running.)
        std::chrono::sys_seconds last_attempt_time; // when the attempt started or ended
        std::string last_attempt_replica;
        std::string last_attempt_error;
        bool last_attempt_succeeded = false;
        /// If an attempt is in progress, this contains error from the previous attempt.
        /// Useful if we keep retrying and failing, and each attempt takes a while - we want to see an error message
        /// without having to catch the brief time window between attempts.
        std::string previous_attempt_error;

        /// Incremented when a refresh attempt starts. Set to 0 when refresh succeeds or when we skip a timeslot.
        /// Used for exponential backoff on errors.
        Int64 attempt_number = 0;

        /// Random number in [-1e9, 1e9], for RANDOMIZE FOR. Re-rolled after every refresh attempt.
        /// (Why write it to keeper instead of letting each replica toss its own coin? Because then refresh would happen earlier
        /// on average, on the replica that generated the shortest delay. We could use nonuniform distribution to compensate, but this is easier.)
        Int64 randomness = 0;

        /// Whether any replica is executing a refresh right now.
        /// May be inaccurate if the replica that's executing refresh lost zookeeper connection for
        /// a long time (long enough for its ephemeral znode to expire + grace period).
        bool refresh_running = false;

        std::chrono::sys_time<std::chrono::nanoseconds> last_success_end_time {};

        /// State of views that this view DEPENDS ON, as of the start of last successful refresh.
        /// Used for triggering dependent refresh: if the last_success_end_time stored here is less than
        /// the dependency's latest last_success_end_time, we should start a refresh.
        AllDependenciesInfo last_success_dependencies;

        /// Znode version. Not serialized.
        int32_t version = -1;

        void randomize(); // assigns `randomness`

        String toString() const;
        void parse(const String & data, bool running_znode_exists, const LoggerPtr & log_);
    };

    /// Just for observability.
    struct Info
    {
        StorageID view_id = StorageID::createEmpty();
        RefreshState state;
        std::chrono::system_clock::time_point next_refresh_time;
        CoordinationZnode znode;
        String replica_name;
        bool refresh_running;
        ProgressValues progress;
        std::optional<String> unexpected_error; // refreshing is stopped because of unexpected error
    };

    /// Never call it manually, public for shared_ptr construction only
    RefreshTask(StorageMaterializedView * view_, ContextPtr context, const ASTRefreshStrategy & strategy, std::vector<StorageID> initial_dependencies_, bool attach, bool coordinated, bool empty, bool is_restore_from_backup);

    /// If !attach, creates coordination znodes if needed.
    static OwnedRefreshTask create(
        StorageMaterializedView * view,
        ContextMutablePtr context,
        const DB::ASTRefreshStrategy & strategy,
        bool attach,
        bool coordinated,
        bool empty,
        bool is_restore_from_backup);

    /// Called at most once.
    void startup();
    void finalizeRestoreFromBackup();
    /// Permanently disable task scheduling and remove this table from RefreshSet.
    /// Ok to call multiple times, including in parallel.
    /// Ok to call even if startup() wasn't called or failed.
    void shutdown();
    /// Call when dropping the table, after shutdown(). Removes coordination znodes if needed.
    void drop(ContextPtr context, bool is_shared_db);
    /// Call when renaming the materialized view.
    void rename(StorageID new_id, StorageID new_inner_table_id);
    /// Call when changing refresh params (ALTER MODIFY REFRESH).
    void checkAlterIsPossible(const DB::ASTRefreshStrategy & new_strategy);
    void alterRefreshParams(const DB::ASTRefreshStrategy & new_strategy);

    Info getInfo() const;
    String getCoordinationPath() const { return coordination.path; }

    bool canCreateOrDropOtherTables() const;

    /// Methods to pause/unpause refreshing on this replica or all replicas.
    /// The per-replica pause and global pause are two separate flags; if either of them is set,
    /// no refreshes will run.
    void start();
    void stop();
    /// Like `stop`, but does not interrupt the currently running refresh; only prevents future
    /// refreshes from starting. Resumed by `start`.
    void pause();
    void startReplicated();
    void stopReplicated(const String & reason);

    /// Schedule task immediately
    void run();
    /// Cancel task execution
    void cancel();

    /// Waits for the currently running refresh attempt to complete, either on this replica
    /// or on another one (if `coordinated`).
    /// If the refresh fails, throws an exception.
    /// If no refresh is running, completes immediately, throwing an exception if previous refresh failed.
    void wait(const ContextPtr & context);

    /// Wait for background work (refreshing or scheduling) on this replica to complete.
    /// Returns false if `deadline` was reached before the work completed. Used by server shutdown.
    bool tryJoinBackgroundTask(std::chrono::steady_clock::time_point deadline);

    /// Information needed for views that depend on this one.
    /// Doesn't assign `table` field.
    DependencyRefreshInfo getInfoForDependentViews() const;

    /// Called when refresh scheduling needs to be reconsidered, e.g. after a refresh happens in
    /// any task that this task depends on.
    void notify();

    /// For tests
    void setFakeTime(std::optional<Int64> t);

    /// RefreshSet will set handle for refresh tasks, to avoid race condition.
    void setRefreshSetHandleUnlock(RefreshSet::Handle && set_handle_);

    /// Looks up the table, does lockForShare() on it. Handles corner cases:
    ///  * If the table was EXCHANGEd+dropped between the lookup and the lockForShare(), try again.
    ///  * If the target table is replicated, and another replica did a refresh, do an equivalent of
    ///    SYSTEM SYNC REPLICA before first read from this table, to make sure we see the data.
    std::tuple<StoragePtr, TableLockHolder> getAndLockTargetTable(const StorageID & storage_id, const ContextPtr & context);

    /// Ensures the output of last successful refresh (as reported to a previous
    /// getInfoForDependentViews() call) is visible to SELECTs on this replica.
    /// Called by dependent views before starting refresh.
    void syncForDependentRefresh(const ContextPtr & context);

private:
    struct CoordinationState
    {
        /// When coordination is enabled, we have these znodes in Keeper:
        ///
        /// keeper_path (CoordinationZnode)
        /// ├── "replicas"
        /// │   ├── name1
        /// │   ├── name2
        /// │   └── name3
        /// ├── ["running"] (ephemeral)
        /// └── ["paused"]

        struct WatchState
        {
            std::atomic_bool should_reread_znodes {true};
            std::atomic_bool root_watch_active {false};
            std::atomic_bool children_watch_active {false};
        };

        CoordinationZnode root_znode;
        bool running_znode_exists = false;
        bool paused_znode_exists = false;
        std::shared_ptr<WatchState> watches = std::make_shared<WatchState>();

        /// Time when we first saw that `root_znode.refresh_running && !running_znode_exists`.
        /// If far enough in the past, the replica that started the refresh has probably crashed,
        /// and we should update the znode to say refresh_running = false.
        std::optional<std::chrono::system_clock::time_point> running_znode_missing_since {};

        /// Whether we use Keeper to coordinate refresh across replicas. If false, we don't write to Keeper,
        /// but we still use the same in-memory structs (CoordinationZnode etc), as if it's coordinated (with one replica).
        bool coordinated = false;

        bool read_only = false;
        String path;
        String replica_name;

        DependencyRefreshInfo notified_dependents;
    };

    /// Information about the currently running refresh.
    struct ExecutionState
    {
        enum class State
        {
            None,
            /// doScheduling() decided to run a refresh, executeRefresh() didn't start yet.
            Requested,
            /// executeRefresh() is in progress.
            Running,
            /// executeRefresh() completed, doScheduling() didn't propagate the result to zookeeper yet.
            Finished,
        };

        /// Protects interrupt_execution and executor.
        /// Can be locked while holding `mutex`.
        std::mutex executor_mutex;
        /// If there's a refresh in progress, it can be aborted by setting this flag and cancel()ling
        /// this executor. Refresh task will then reconsider what to do, re-checking `stop_requested`,
        /// `out_of_schedule_refresh_requested`, etc.
        std::atomic_bool interrupt_execution {false};
        PipelineExecutor * executor = nullptr;
        /// Interrupts internal CREATE/EXCHANGE/DROP queries that refresh does. Only used during shutdown.
        StopSource cancel_ddl_queries;
        Progress progress;

        State state = State::None;
        /// Contains information about the completed refresh, and znode version number at the start
        /// of refresh.
        CoordinationZnode znode;
        std::chrono::system_clock::time_point start_time;
        /// State of dependencies that was used for triggering this refresh.
        /// Should be writtent to zookeeper only if the refresh succeeds.
        AllDependenciesInfo dependencies;
        bool out_of_schedule = false;
    };

    struct SchedulingState
    {
        /// Refreshes are stopped, e.g. by SYSTEM STOP VIEW or SYSTEM PAUSE VIEW.
        /// We shouldn't start new refreshes, but pre-existing refresh attempt may keep going.
        bool stop_requested = false;
        /// Refreshes are stopped because we got an unexpected error. Can be resumed with SYSTEM START VIEW.
        std::optional<String> unexpected_error;
        /// An out-of-schedule refresh was requested, e.g. by SYSTEM REFRESH VIEW.
        bool out_of_schedule_refresh_requested = false;

        /// Solves this unusual case:
        /// View X: REFRESH EVERY 10 SECOND.
        /// View Y: REFRESH AFTER 20 SECOND DEPENDS ON X.
        /// If we interpret "AFTER 20 SECOND" as "it's been >= 20 seconds since the latest refresh of X",
        /// this condition will ~never be satisfied as latest refresh time will advance every 10 seconds.
        /// Instead, we have to store X's latest refresh time once (after it advances since Y's refresh)
        /// and count 20 seconds from that. This is where we store it.
        std::unordered_map<String, std::chrono::sys_time<std::chrono::nanoseconds>> seen_dep_refresh_times;

        /// Used in tests. If not INT64_MIN, we pretend that this is the current time, instead of calling system_clock::now().
        std::atomic<Int64> fake_clock {INT64_MIN};
    };

    std::mutex logger_mutex;
    LoggerPtr current_logger = nullptr;

    StorageMaterializedView * view;

    /// Protects all fields below.
    /// Never locked for blocking operations (e.g. creating the internal table or reading from zookeeper).
    /// Can't be locked while holding `executor_mutex`.
    mutable std::mutex mutex;

    RefreshSchedule refresh_schedule;
    RefreshSettings refresh_settings;
    std::vector<StorageID> initial_dependencies;
    const bool refresh_append;

    RefreshSet::Handle set_handle;

    /// Calls doScheduling() from background thread.
    BackgroundSchedulePoolTaskHolder scheduling_task;
    /// Calls executeRefresh() from background thread.
    BackgroundSchedulePoolTaskHolder execution_task;

    Coordination::WatchCallbackPtr watch_callback;

    CoordinationState coordination;
    ExecutionState execution;
    SchedulingState scheduling;

    RefreshState state = RefreshState::Scheduling;
    /// Notified when wait() needs to wake up: when `state` or `root_znode` changes, and on shutdown.
    std::condition_variable wait_cv;
    std::chrono::system_clock::time_point next_refresh_time {}; // just for observability

    /// If we're in a Replicated database, and another replica performed a refresh, we have to do an
    /// equivalent of SYSTEM SYNC REPLICA on the new table to make sure we see the full data.
    /// We do this different in two cases:
    ///  * In non-APPEND mode, we sync-if-needed on each SELECT from the target table, in getAndLockTargetTable.
    ///    Sync if the table's UUID has changed (last_synced_inner_uuid).
    ///  * In APPEND mode, we sync-if-needed only before dependent view refresh (i.e. if there's
    ///    another view that DEPENDS ON this view), in syncForDependentRefresh.
    ///    Sync if root_znode.last_success_end_time has changed (last_synced_refresh_end_time).
    std::mutex replica_sync_mutex;
    UUID last_synced_inner_uuid = UUIDHelpers::Nil;
    std::chrono::sys_time<std::chrono::nanoseconds> last_synced_refresh_end_time {};

    /// We have two "threads": one for managing scheduling and zookeeper state, one for executing
    /// refresh. Two are needed because we may need to update zookeeper state during refresh.
    ///
    /// doScheduling(), started through scheduling_task, is the scheduling / zookeeper management.
    /// It runs whenever anything changes (e.g. znodes change, or refresh completes, or retry timer fires).
    /// It looks at the state of everything and decides what needs to be done.
    /// Public methods just provide inputs for the doScheduling()'s decisions
    /// (e.g. stop_requested, out_of_schedule_refresh_requested), they don't do anything significant themselves.
    /// If is_shutdown, both background tasks were stopped, and we only need to write to zookeeper
    /// to reflect that this replica is not running a refresh anymore.
    ///
    /// executeRefresh(), started through execution_task, is where actual refresh SQL query runs.
    void doScheduling(bool is_shutdown);
    void executeRefresh();

    /// Perform an actual refresh: create new table, run INSERT SELECT, exchange tables, drop old table.
    /// Mutex must be unlocked.
    std::optional<UUID> executeRefreshUnlocked(int32_t root_znode_version, std::vector<StorageID> deps, const String & log_comment, String & out_error_message);

    DependencyRefreshInfo getInfoForDependentViewsLocked(const std::unique_lock<std::mutex> &) const;

    /// Call after root_znode.last_success_end_time may have changed.
    void notifyDependentsIfNeeded(std::unique_lock<std::mutex> & lock);

    /// Look up the IStorage-s for views we depend on and get their in-memory information about latest refreshes.
    /// Returns nonempty list iff there are any dependencies.
    bool collectDependencyStates(AllDependenciesInfo & out, std::unique_lock<std::mutex> & lock);
    bool collectDependencyStatesUnlocked(AllDependenciesInfo & out, const std::vector<StorageID> & deps);
    void syncDependenciesForRefresh(const std::vector<StorageID> & deps, const ContextPtr & context);

    /// Looks at time and dependencies and decides when to do next refresh.
    /// Returns:
    ///  * When to refresh, according to schedule. The caller may ignore it and refresh right away.
    ///    If max(), we're waiting for dependencies.
    ///  * Whether we shouldn't actually refresh now because dependencies are not satisfied.
    ///    This is separate from the time_point::max() above because we'd like system.view_refreshes
    ///    to show next scheduled refresh time even if dependencies are not satisfied yet.
    ///  * What to write to zookeeper if we do start a refresh.
    ///    Valid regardless of the scheduled time as the caller may ignore schedule and refresh anyway,
    ///    e.g. on SYSTEM REFRESH VIEW.
    std::tuple<std::chrono::system_clock::time_point, bool /*waiting_for_dependencies*/, CoordinationZnode>
    determineNextRefreshTime(std::chrono::system_clock::time_point now, const AllDependenciesInfo & dependencies, const std::unique_lock<std::mutex> & lock);

    void readZnodesIfNeeded(std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::unique_lock<std::mutex> & lock);
    /// Update the root znode and create/remove-if-exists the 'running' znode,
    /// atomically, conditionally on the root znode version number.
    /// If `only_running_znode`, the root znode is not updated, but its version is still checked.
    /// If version number doesn't match, schedules a doScheduling() call
    /// with should_reread_znodes = true, and returns false.
    /// If coordination is disabled, just update in-memory struct without writing to zookeeper.
    bool updateCoordinationState(CoordinationZnode root, bool running, std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::unique_lock<std::mutex> & lock, bool only_running_znode = false);

    void setState(RefreshState s, std::unique_lock<std::mutex> & lock);
    void scheduleRefresh(std::lock_guard<std::mutex> & lock);
    void interruptExecution();
    std::chrono::system_clock::time_point currentTime() const;

    void waitForLatestTargetTable(const ContextPtr & context);

    void createLogger(const StorageID & storage_id);
    LoggerPtr getLogger();
};

using RefreshTaskPtr = std::shared_ptr<RefreshTask>;

/// Wrapper around shared_ptr<RefreshTask>, calls shutdown() in destructor.
struct OwnedRefreshTask
{
    RefreshTaskPtr ptr;

    OwnedRefreshTask() = default;
    explicit OwnedRefreshTask(RefreshTaskPtr p) : ptr(std::move(p)) {}
    OwnedRefreshTask(OwnedRefreshTask &&) = default;
    OwnedRefreshTask & operator=(OwnedRefreshTask &&) = default;

    ~OwnedRefreshTask() { if (ptr) ptr->shutdown(); }

    RefreshTask* operator->() const { return ptr.get(); }
    RefreshTask& operator*() const { return *ptr; }
    explicit operator bool() const { return ptr != nullptr; }
};
}
