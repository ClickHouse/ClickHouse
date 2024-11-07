#pragma once

#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MaterializedView/RefreshSchedule.h>
#include <Storages/MaterializedView/RefreshSettings.h>
#include <Common/StopToken.h>
#include <Core/BackgroundSchedulePool.h>

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
    Running,
    RunningOnAnotherReplica,
};

class RefreshTask : public std::enable_shared_from_this<RefreshTask>
{
public:
    struct Info;

    /// Never call it manually, public for shared_ptr construction only
    RefreshTask(StorageMaterializedView * view_, ContextPtr context, const ASTRefreshStrategy & strategy, bool attach, bool coordinated, bool empty);

    /// If !attach, creates coordination znodes if needed.
    static OwnedRefreshTask create(
        StorageMaterializedView * view,
        ContextMutablePtr context,
        const DB::ASTRefreshStrategy & strategy,
        bool attach,
        bool coordinated,
        bool empty);

    /// Called at most once.
    void startup();
    /// Permanently disable task scheduling and remove this table from RefreshSet.
    /// Ok to call multiple times, but not in parallel.
    /// Ok to call even if startup() wasn't called or failed.
    void shutdown();
    /// Call when dropping the table, after shutdown(). Removes coordination znodes if needed.
    void drop(ContextPtr context);
    /// Call when renaming the materialized view.
    void rename(StorageID new_id, StorageID new_inner_table_id);
    /// Call when changing refresh params (ALTER MODIFY REFRESH).
    void checkAlterIsPossible(const DB::ASTRefreshStrategy & new_strategy);
    void alterRefreshParams(const DB::ASTRefreshStrategy & new_strategy);

    Info getInfo() const;

    /// Enable task scheduling
    void start();
    /// Disable task scheduling
    void stop();
    /// Schedule task immediately
    void run();
    /// Cancel task execution
    void cancel();

    /// Waits for the currently running refresh attempt to complete.
    /// If the refresh fails, throws an exception.
    /// If no refresh is running, completes immediately, throwing an exception if previous refresh failed.
    void wait();

    /// A measure of how far this view has progressed. Used by dependent views.
    std::chrono::sys_seconds getNextRefreshTimeslot() const;

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

    struct CoordinationZnode
    {
        /// "Official" time of the latest successful refresh, i.e. time according to schedule rather than wall clock,
        /// and without randomization. E.g. for REFRESH EVERY 1 DAY this timestamp is always the first second of a
        /// calendar day. 0 if no successful refresh happened yet.
        /// (We store last rather than next timeslot because it behaves better when ALTER MODIFY REFRESH reduces refresh period.)
        std::chrono::sys_seconds last_completed_timeslot;

        /// Time when the latest successful refresh started.
        std::chrono::sys_seconds last_success_time;
        std::chrono::milliseconds last_success_duration;
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
        /// on average, on the replica that generated the shortest delay. We could use nonuniform distribution to complensate, but this is easier.)
        Int64 randomness = 0;

        /// Znode version. Not serialized.
        int32_t version = -1;

        void randomize(); // assigns `randomness`

        String toString() const;
        void parse(const String & data);
    };

    /// Just for observability.
    struct Info
    {
        StorageID view_id = StorageID::createEmpty();
        RefreshState state;
        std::chrono::system_clock::time_point next_refresh_time;
        CoordinationZnode znode;
        bool refresh_running;
        ProgressValues progress;
    };

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
        /// └── ["running"] (RunningZnode, ephemeral)

        struct WatchState
        {
            std::atomic_bool should_reread_znodes {true};
            std::atomic_bool root_watch_active {false};
            std::atomic_bool children_watch_active {false};
        };

        CoordinationZnode root_znode;
        bool running_znode_exists = false;
        std::shared_ptr<WatchState> watches = std::make_shared<WatchState>();

        /// Whether we use Keeper to coordinate refresh across replicas. If false, we don't write to Keeper,
        /// but we still use the same in-memory structs (CoordinationZnode etc), as if it's coordinated (with one replica).
        bool coordinated = false;
        bool read_only = false;
        String path;
        String replica_name;
    };

    struct ExecutionState
    {
        /// Protects interrupt_execution and executor.
        /// Can be locked while holding `mutex`.
        std::mutex executor_mutex;
        /// If there's a refresh in progress, it can be aborted by setting this flag and cancel()ling
        /// this executor. Refresh task will then reconsider what to do, re-checking `stop_requested`,
        /// `cancel_requested`, etc.
        std::atomic_bool interrupt_execution {false};
        PipelineExecutor * executor = nullptr;
        /// Interrupts internal CREATE/EXCHANGE/DROP queries that refresh does. Only used during shutdown.
        StopSource cancel_ddl_queries;
        Progress progress;
    };

    struct SchedulingState
    {
        /// Refreshes are stopped, e.g. by SYSTEM STOP VIEW.
        bool stop_requested = false;
        /// An out-of-schedule refresh was requested, e.g. by SYSTEM REFRESH VIEW.
        bool out_of_schedule_refresh_requested = false;

        /// Timestamp representing the progress of refreshable views we depend on. We're allowed to do
        /// refreshes for timeslots <= dependencies_satisfied_until without waiting for dependencies.
        /// If negative, we should recalculate this value.
        std::chrono::sys_seconds dependencies_satisfied_until {std::chrono::seconds(-1)};

        /// Used in tests. If not INT64_MIN, we pretend that this is the current time, instead of calling system_clock::now().
        std::atomic<Int64> fake_clock {INT64_MIN};
    };

    LoggerPtr log = nullptr;
    StorageMaterializedView * view;

    /// Protects all fields below.
    /// Never locked for blocking operations (e.g. creating or dropping the internal table).
    /// Can't be locked while holding `executor_mutex`.
    mutable std::mutex mutex;

    RefreshSchedule refresh_schedule;
    RefreshSettings refresh_settings;
    std::vector<StorageID> initial_dependencies;
    bool refresh_append;
    bool in_database_replicated;

    RefreshSet::Handle set_handle;

    /// Calls refreshTask() from background thread.
    BackgroundSchedulePool::TaskHolder refresh_task;

    CoordinationState coordination;
    ExecutionState execution;
    SchedulingState scheduling;

    RefreshState state = RefreshState::Scheduling;
    /// Notified when `state` changes away from Running/Scheduling.
    std::condition_variable refresh_cv;
    std::chrono::system_clock::time_point next_refresh_time {}; // just for observability

    /// If we're in a Replicated database, and another replica performed a refresh, we have to do an
    /// equivalent of SYSTEM SYNC REPLICA on the new table to make sure we see the full data.
    std::mutex replica_sync_mutex;
    UUID last_synced_inner_uuid = UUIDHelpers::Nil;

    /// The main loop of the refresh task. It examines the state, sees what needs to be
    /// done and does it. If there's nothing to do at the moment, returns; it's then scheduled again,
    /// when needed, by public methods or by timer.
    ///
    /// Public methods just provide inputs for the refreshTask()'s decisions
    /// (e.g. stop_requested, cancel_requested), they don't do anything significant themselves.
    void refreshTask();

    /// Perform an actual refresh: create new table, run INSERT SELECT, exchange tables, drop old table.
    /// Mutex must be unlocked. Called only from refresh_task.
    UUID executeRefreshUnlocked(bool append, int32_t root_znode_version);

    /// Assigns dependencies_satisfied_until.
    void updateDependenciesIfNeeded(std::unique_lock<std::mutex> & lock);

    std::tuple<std::chrono::system_clock::time_point, std::chrono::sys_seconds, CoordinationZnode>
    determineNextRefreshTime(std::chrono::sys_seconds now);

    void readZnodesIfNeeded(std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::unique_lock<std::mutex> & lock);
    bool updateCoordinationState(CoordinationZnode root, bool running, std::shared_ptr<zkutil::ZooKeeper> zookeeper, std::unique_lock<std::mutex> & lock);
    void removeRunningZnodeIfMine(std::shared_ptr<zkutil::ZooKeeper> zookeeper);

    void setState(RefreshState s, std::unique_lock<std::mutex> & lock);
    void interruptExecution();
    std::chrono::system_clock::time_point currentTime() const;
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
