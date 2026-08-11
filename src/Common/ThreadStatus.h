#pragma once

#include <Core/LogsLevel.h>
#include <IO/Progress.h>
#include <Interpreters/Context_fwd.h>
#include <Common/IThrottler.h>
#include <Common/Logger_fwd.h>
#include <Common/MemoryTracker.h>
#include <Common/PerCPUMemoryThreadState.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/MemorySpillScheduler.h>
#include <Common/UntrackedMemoryRegistry.h>

#include <boost/noncopyable.hpp>

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_set>
#include <vector>


template <class T>
class ConcurrentBoundedQueue;

namespace DB
{

class QueryStatus;
class ThreadStatus;
class QueryProfilerReal;
class QueryProfilerCPU;
class QueryThreadLog;
class TasksStatsCounters;
struct RUsageCounters;
struct PerfEventsCounters;
class InternalTextLogsQueue;
struct ViewRuntimeData;
class QueryViewsLog;
struct Settings;
enum class ThreadName : uint8_t;

/// Apply memory-profiler / fault-injection / soft-limit related query settings to a `MemoryTracker`.
/// Query-level sample settings (`memory_profiler_*`) are pushed only when they were actually changed
/// from their default — otherwise the tracker is left at `sample_probability == -1` so that
/// `getResolvedSampleConfig` transparently falls through to `total_memory_tracker_sample_probability`.
void configureMemoryTrackerFromSettings(bool has_trace_collector, MemoryTracker & memory_tracker, const Settings & settings);

using InternalTextLogsQueuePtr = std::shared_ptr<InternalTextLogsQueue>;
using InternalTextLogsQueueWeakPtr = std::weak_ptr<InternalTextLogsQueue>;

using InternalProfileEventsQueue = ConcurrentBoundedQueue<Block>;
using InternalProfileEventsQueuePtr = std::shared_ptr<InternalProfileEventsQueue>;
using InternalProfileEventsQueueWeakPtr = std::weak_ptr<InternalProfileEventsQueue>;

using QueryIsCanceledPredicate = std::function<bool()>;
/// Throws the real cancellation cause if the query has been cancelled and its process-list element is available.
using ThrowIfQueryCanceledPredicate = std::function<void()>;

/** Thread group is a collection of threads dedicated to single task
  * (query or other process like background merge).
  *
  * ProfileEvents (counters) from a thread are propagated to thread group.
  *
  * Create via CurrentThread::initializeQuery (for queries) or directly (for various background tasks).
  * Use via CurrentThread::getGroup.
  *
  * A group either owns its accounting or borrows it. A borrowed group (`createForMaterializedView` /
  * `createForFlushAsyncInsertQueue` / `createForExplainAnalyze`) sets its `performance_counters` and
  * `memory_tracker` to raw (non-owning) pointers into the parent query group, so materialized-view,
  * async-insert and `EXPLAIN ANALYZE` work is accounted against the parent query.
  *
  * Borrowing a raw pointer (rather than holding a `shared_ptr` to the parent) is deliberate: owning the
  * parent would keep the finished query's group - and its memory accounting - alive for as long as the
  * borrowed work runs. The price is that a borrowed group is only valid while the parent is alive, so it
  * must not be captured by asynchronous work: the async task may run on a pool thread after the parent
  * query finished and its group was destroyed, and would then dereference the freed parent counters
  * (use-after-free). `getCurrentThreadGroupForAsyncCallback` therefore never returns a borrowed group;
  * for a borrowed group it returns its async-callback companion (see `getAsyncCallbackGroup`), which
  * parents its accounting at the borrowed group's own (live) counters and holds a `shared_ptr` to every
  * group that chain traverses - the borrowed group itself, any intermediate borrowed groups, and the
  * owning query group. Async work thus still charges the accounting chain the borrowed scope itself
  * charges - `max_memory_usage` / `max_memory_usage_for_user` keep applying and frees are credited back
  * to the query - without dereferencing a freed group, and the query's cancellation predicates and
  * metadata are preserved. The companion is created lazily on the first async capture, so a borrowed
  * group whose scope schedules no async work does not prolong any lifetime at all.
  *
  * Pointing at the borrowed group's own accounting rather than directly at the owner matters because
  * that chain can be re-pointed after the borrowed group is created: `ProcessList::insert` reparents the
  * current group's `memory_tracker` to the query/user trackers of the query being inserted, which for a
  * `createForFlushAsyncInsertQueue` group happens after construction (the flush builds its own process
  * list entry). Caching the construction-time owner would charge such async work to the outer caller's
  * query and bypass the flush query's own limits.
  */
class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

class ThreadGroup
{
public:
    using FatalErrorCallback = std::function<void()>;
    ThreadGroup(ContextPtr query_context_, Int32 os_threads_nice_value_, FatalErrorCallback fatal_error_callback_ = {});

    ~ThreadGroup();

    /// A borrowed group aliases the parent query group's accounting; see the class comment.
    bool isBorrowed() const;

    /// For a borrowed group: an owning companion group that async callbacks may safely capture.
    /// It carries the query metadata and cancellation predicates of the borrowed scope (so canceling
    /// the query still stops async work promptly) and parents its `memory_tracker` /
    /// `performance_counters` at `borrowed`'s own ones, holding every group that accounting chain
    /// traverses alive - so async work still charges whatever the borrowed scope charges and stays
    /// subject to its memory limits, without dereferencing a freed group.
    /// Created lazily on the first call. Returns nullptr for a non-borrowed group.
    /// Static because the companion needs to share ownership of `borrowed` itself.
    static ThreadGroupPtr getAsyncCallbackGroup(const ThreadGroupPtr & borrowed);

    /// Whether an async-callback companion that charges this group's accounting is still alive, so
    /// late async work may still charge it - and, through the `memory_tracker` parent chain, the user
    /// trackers (see `ProcessListForUser::lingering_query_groups`).
    bool hasLiveAsyncCallbackCompanions() const;

    /// The first thread created this thread group
    const UInt64 master_thread_id;

    /// Set up at creation, no race when reading
    const ContextWeakPtr query_context;
    const ContextWeakPtr global_context;

    const FatalErrorCallback fatal_error_callback;

    const Int32 os_threads_nice_value;

    MemorySpillScheduler::Ptr memory_spill_scheduler;

    /// For a borrowed group these are raw pointers into the parent query group (see the class comment).
    ProfileEvents::Counters performance_counters{VariableContext::Process};
    MemoryTracker memory_tracker{VariableContext::Process};

    struct SharedData
    {
        InternalProfileEventsQueueWeakPtr profile_queue_ptr;

        InternalTextLogsQueueWeakPtr logs_queue_ptr;
        LogsLevel client_logs_level = LogsLevel::none;

        String query_for_logs;
        UInt64 normalized_query_hash = 0;

        // Since processors might be added on the fly within expand() function we use atomic_size_t.
        // These two fields are used for EXPLAIN PLAN / PIPELINE.
        std::shared_ptr<std::atomic_size_t> plan_step_index = std::make_shared<std::atomic_size_t>(0);
        std::shared_ptr<std::atomic_size_t> pipeline_processor_index = std::make_shared<std::atomic_size_t>(0);

        QueryIsCanceledPredicate query_is_canceled_predicate = {};
        ThrowIfQueryCanceledPredicate throw_if_query_canceled_predicate = {};
    };

    SharedData getSharedData()
    {
        /// Critical section for making the copy of shared_data
        std::lock_guard lock(mutex);
        return shared_data;
    }

    /// Mutation shared data
    void attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue, LogsLevel logs_level);
    void attachQueryForLog(const String & query_, UInt64 normalized_hash = 0);
    void attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & profile_queue);

    /// When new query starts, new thread group is created for it, current thread becomes master thread of the query
    static ThreadGroupPtr createForQuery(ContextPtr query_context_, FatalErrorCallback fatal_error_callback_ = {});

    /// NOTE: The caller should call background_memory_tracker.adjustOnBackgroundTaskEnd() at the end (see existing callers),
    /// and make sure that you are the only user of this shared_ptr (usually it is managed via ThreadGroupSwitcher)
    static ThreadGroupPtr createForMergeMutate(ContextPtr storage_context);

    static ThreadGroupPtr createForMaterializedView(ContextPtr context);
    static ThreadGroupPtr createForFlushAsyncInsertQueue(ContextPtr context, ThreadGroupPtr parent);
    static ThreadGroupPtr createForExplainAnalyze(ThreadGroupPtr parent);

    std::vector<UInt64> getInvolvedThreadIds() const;
    size_t getPeakThreadsUsage() const;
    UInt64 getGroupElapsedMs() const;

    void linkThread(UInt64 thread_id);
    void unlinkThread();

private:
    enum class ThreadGroupKind : uint8_t
    {
        Root,     /// Owns `performance_counters` / `memory_tracker`.
        Borrowed, /// Aliases the parent group's accounting; valid only while the parent lives.
    };

    const ThreadGroupKind kind = ThreadGroupKind::Root;

    mutable std::mutex mutex;

    /// Set up at creation, no race when reading
    SharedData shared_data TSA_GUARDED_BY(mutex);

    /// Set of all thread ids which has been attached to the group
    std::unordered_set<UInt64> thread_ids TSA_GUARDED_BY(mutex);

    /// Count of simultaneously working threads
    size_t active_thread_count TSA_GUARDED_BY(mutex) = 0;

    /// Peak threads count in the group
    size_t peak_threads_usage TSA_GUARDED_BY(mutex) = 0;

    Stopwatch effective_group_stopwatch TSA_GUARDED_BY(mutex) = Stopwatch(STOPWATCH_DEFAULT_CLOCK, 0, /* is running */ false);
    UInt64 elapsed_group_ms TSA_GUARDED_BY(mutex) = 0;

    /// Set only for borrowed groups: the group this one's raw accounting pointers point at (itself
    /// possibly borrowed). Weak on purpose: a borrowed group alone must not prolong its parent.
    std::weak_ptr<ThreadGroup> borrowed_accounting_parent;

    /// Set only for borrowed groups: the companion returned by `getAsyncCallbackGroup`, created lazily
    /// on the first async capture. Weak because the companion shares ownership of this group (it
    /// parents its accounting at this group's counters), so a strong link here would be a cycle. The
    /// companion is kept alive by the async callbacks that captured it; once they are all done it may
    /// expire and the next capture creates a fresh one.
    std::weak_ptr<ThreadGroup> async_callback_group TSA_GUARDED_BY(mutex);

    /// Set only for async-callback companions: every group whose accounting objects this group's
    /// `memory_tracker` / `performance_counters` parent chain traverses - the borrowed group itself,
    /// any intermediate borrowed groups, and the owning query group - kept alive while async work may
    /// charge them. Ordered from the borrowed group to the owner.
    std::vector<ThreadGroupPtr> companion_accounting_chain;

    /// Set only for async-callback companions: how many leading groups of `companion_accounting_chain`
    /// the live memory-tracker chain traverses. Only they get `live_async_callback_companions` bumped;
    /// the rest of the chain is held for lifetime only (`performance_counters` are never re-pointed,
    /// but `ProcessList::insert` may re-point `memory_tracker` - see `getAsyncCallbackGroup`).
    size_t companion_counted_groups = 0;

    /// Number of live async-callback companions whose accounting chain includes this group.
    std::atomic<size_t> live_async_callback_companions = 0;

    /// Borrowing constructors (mark the group `Borrowed`); private so only the factories create them.
    explicit ThreadGroup(ThreadGroupPtr parent);
    ThreadGroup(ContextPtr query_context_, ThreadGroupPtr parent);

    /// Constructor for the async-callback companion of a borrowed group (see `getAsyncCallbackGroup`).
    struct AsyncCallbackCompanionTag {};
    ThreadGroup(
        SharedData borrowed_shared_data, std::vector<ThreadGroupPtr> accounting_chain, size_t counted_groups, AsyncCallbackCompanionTag);

    static ThreadGroupPtr create(ContextPtr context, Int32 os_threads_nice_value);
};

/** Encapsulates all per-thread info (ProfileEvents, MemoryTracker, query_id, query context, etc.).
  * The object must be created in thread function and destroyed in the same thread before the exit.
  * It is accessed through thread-local pointer.
  *
  * This object should be used only via "CurrentThread", see CurrentThread.h
  */
class ThreadStatus : public boost::noncopyable
{
public:
    /// Linux's PID (or TGID) (the same id is shown by ps util)
    const UInt64 thread_id = 0;

    /// TODO: merge them into common entity
    ProfileEvents::Counters performance_counters{VariableContext::Thread};
    /// Points to performance_counters by default.
    /// Could be changed to point to another object to calculate performance counters for some narrow scope.
    ProfileEvents::Counters * current_performance_counters{&performance_counters};

    MemoryTracker memory_tracker{VariableContext::Thread};
    /// Small amount of untracked memory (per thread atomic-less counter)
    UntrackedMemoryCounter untracked_memory;
    /// MemoryTrackerBlockerInThread state corresponding to untracked_memory.
    VariableContext untracked_memory_blocker_level = VariableContext::Max;
    /// Each thread could new/delete memory in range of (-untracked_memory_limit, untracked_memory_limit) without access to common counters.
    Int64 untracked_memory_limit = 4 * 1024 * 1024;
    /// Per-CPU untracked memory
    PerCPUMemoryThreadState per_cpu_untracked_memory;

    /// Statistics of read and write rows/bytes
    Progress progress_in;
    Progress progress_out;

    /// IO scheduling and throttling
    ResourceLink read_resource_link;
    ResourceLink write_resource_link;
    ThrottlerPtr read_throttler;
    ThrottlerPtr write_throttler;

protected:
    /// Group of threads, to which this thread attached
    ThreadGroupPtr thread_group;

    /// Is set once
    ContextWeakPtr global_context;
    /// Use it only from current thread
    ContextWeakPtr query_context;

    /// Is used to send logs from logs_queue to client in case of fatal errors.
    using FatalErrorCallback = std::function<void()>;
    FatalErrorCallback fatal_error_callback;

    ThreadGroup::SharedData local_data;

    bool performance_counters_finalized = false;

    String query_id;
    /// The query_id can be read by signal handlers. If the signal interrupts the thread while it is updating the query_id, it can lead to a race.
    std::atomic<bool> is_query_id_usable{true};
    /// is_query_id_usable is used in signal handlers, so ensure it is lock-free to avoid undefined behavior in signal handlers.
    static_assert(std::atomic<bool>::is_always_lock_free);

    [[maybe_unused]] bool jemalloc_profiler_enabled = false;

    struct TimePoint
    {
        void setUp();
        UInt64 nanoseconds() const;
        UInt64 microseconds() const;
        UInt64 seconds() const;

        UInt64 elapsedMilliseconds() const;
        UInt64 elapsedMilliseconds(const TimePoint & current) const;

        std::chrono::time_point<std::chrono::system_clock> point;
    };

    TimePoint thread_attach_time{};

    // CPU and Real time query profilers
    std::unique_ptr<QueryProfilerReal> query_profiler_real;
    std::unique_ptr<QueryProfilerCPU> query_profiler_cpu;

    /// Use ptr not to add extra dependencies in the header
    std::unique_ptr<RUsageCounters> last_rusage;
    std::unique_ptr<TasksStatsCounters> taskstats;
    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
    UInt64 last_performance_counters_update_time = 0;

    /// This is helpful for cut linking dependencies for clickhouse_common_io
    using Deleter = std::function<void()>;
    Deleter deleter;

    LoggerPtr log = nullptr;

public:
    explicit ThreadStatus();
    ~ThreadStatus();

    ThreadGroupPtr getThreadGroup() const;

    void setQueryId(std::string && new_query_id) noexcept;
    void clearQueryId() noexcept;
    std::string_view getQueryId() const;

    ContextPtr tryGetQueryContext() const;
    ContextPtr getGlobalContext() const;

    /// Attaches slave thread to existing thread group
    void attachToGroup(const ThreadGroupPtr & thread_group_, bool check_detached = true);

    /// Detaches thread from the thread group and the query, dumps performance counters if they have not been dumped
    void detachFromGroup();

    /// Returns pointer to the current profile counters to restore them back.
    /// Note: consequent call with new scope will detach previous scope.
    ProfileEvents::Counters * attachProfileCountersScope(ProfileEvents::Counters * performance_counters_scope);

    void attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue,
                                     LogsLevel client_logs_level);
    InternalTextLogsQueuePtr getInternalTextLogsQueue() const;
    LogsLevel getClientLogsLevel() const;

    void attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & profile_queue);
    InternalProfileEventsQueuePtr getInternalProfileEventsQueue() const;

    void attachQueryForLog(const String & query_);
    const String & getQueryForLog() const;

    bool isQueryCanceled() const;

    /// Throws the real cancellation cause if the query has been cancelled. No-op if not attached to a query.
    void throwIfQueryCanceled() const;

    /// Proper cal for fatal_error_callback
    void onFatalError();

    /// Update several ProfileEvents counters
    void updatePerformanceCounters();
    void updatePerformanceCountersIfNeeded();

    /// Update ProfileEvents and dumps info to system.query_thread_log
    void finalizePerformanceCounters();

    /// Set the counters last usage to now
    void resetPerformanceCountersLastUsage();

    void logToQueryViewsLog(const ViewRuntimeData & vinfo);

    void flushUntrackedMemory();

    void initGlobalProfiler(UInt64 global_profiler_real_time_period, UInt64 global_profiler_cpu_time_period);

    size_t getNextPlanStepIndex() const;
    size_t getNextPipelineProcessorIndex() const;

    double getEffectiveSampleProbability(UInt64 size) const
    {
        if (sample_probability <= 0)
            return 0;
        if (sample_min_allocation_size && size < sample_min_allocation_size)
            return 0;
        if (sample_max_allocation_size && size > sample_max_allocation_size)
            return 0;
        return sample_probability;
    }

    /// getEffectiveSampleProbability reads only this cache on the per-allocation path, so it must be
    /// re-resolved from the tracker chain whenever the effective parent changes (attach, switcher),
    /// otherwise threads parented to total_memory_tracker miss total_memory_tracker_sample_probability.
    MemoryTracker::SampleConfig getMemorySampleConfig() const { return {sample_probability, sample_min_allocation_size, sample_max_allocation_size}; }
    void setMemorySampleConfig(const MemoryTracker::SampleConfig & c)
    {
        sample_probability = c.probability;
        sample_min_allocation_size = c.min_allocation_size;
        sample_max_allocation_size = c.max_allocation_size;
    }
    void resolveMemorySampleConfig() { setMemorySampleConfig(memory_tracker.getResolvedSampleConfig()); }

private:
    void applyGlobalSettings();
    void applyQuerySettings();

    void initPerformanceCounters();

    void initQueryProfiler();

    void finalizeQueryProfiler();

    void logToQueryThreadLog(QueryThreadLog & thread_log, const String & current_database);

    void attachToGroupImpl(const ThreadGroupPtr & thread_group_);

    /// Cached sample probability resolved from MemoryTracker hierarchy to avoid parent traversal on every allocation
    double sample_probability = 0;
    UInt64 sample_min_allocation_size = 0;
    UInt64 sample_max_allocation_size = 0;
};

/**
 * Creates ThreadStatus for the main thread.
 */
class MainThreadStatus : public ThreadStatus
{
public:
    static MainThreadStatus & getInstance();
    static ThreadStatus * get() { return main_thread; }
    static bool initialized() { return is_initialized.test(std::memory_order_relaxed); }

    static void reset() { is_initialized.clear(std::memory_order_relaxed); }

    ~MainThreadStatus();

private:
    MainThreadStatus();

    static ThreadStatus * main_thread;
    static std::atomic_flag is_initialized;
};

}
