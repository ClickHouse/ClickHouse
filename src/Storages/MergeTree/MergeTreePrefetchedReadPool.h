#pragma once
#include <Storages/MergeTree/MergeTreeReadPoolBase.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/ThreadPool_fwd.h>
#include <IO/AsyncReadCounters.h>
#include <boost/heap/priority_queue.hpp>
#include <queue>

namespace DB
{
class IMergeTreeReader;
using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;

class RuntimeDataflowStatisticsCacheUpdater;
using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

/// A class which is responsible for creating read tasks
/// which are later taken by readers via getTask method.
/// Does prefetching for the read tasks it creates.
class MergeTreePrefetchedReadPool : public MergeTreeReadPoolBase
{
public:
    MergeTreePrefetchedReadPool(
        RangesInDataParts && parts_,
        MutationsSnapshotPtr mutations_snapshot_,
        VirtualFields shared_virtual_fields_,
        const IndexReadTasks & index_read_tasks_,
        const StorageSnapshotPtr & storage_snapshot_,
        const FilterDAGInfoPtr & row_level_filter_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    String getName() const override { return "PrefetchedReadPool"; }
    bool preservesOrderOfRanges() const override { return false; }
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;

    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

    static bool checkReadMethodAllowed(LocalFSReadMethod method);
    static bool checkReadMethodAllowed(RemoteFSReadMethod method);

private:
    struct PartStatistic
    {
        size_t sum_marks = 0;

        size_t approx_size_of_mark = 0;
        size_t prefetch_step_marks = 0;

        size_t estimated_memory_usage_for_single_prefetch = 0;
        size_t required_readers_num = 0;
    };

    class PrefetchedReaders
    {
    public:
        /// When `issue_prefetch_synchronously` is true, prefetches are issued inline in the
        /// constructor instead of being scheduled on `pool`. Used by the deferred warmup path,
        /// which already runs on a background thread, so it must not recursively enqueue onto the
        /// same prefetch threadpool (that could self-deadlock when the pool queue is full).
        PrefetchedReaders(
            ThreadPool & pool, MergeTreeReadTask::Readers readers_, Priority priority_, MergeTreePrefetchedReadPool & read_prefetch,
            bool issue_prefetch_synchronously = false);

        void wait();
        MergeTreeReadTask::Readers get();
        bool valid() const { return is_valid; }

    private:
        bool is_valid = false;
        MergeTreeReadTask::Readers readers;

        ThreadPoolCallbackRunnerLocal<void> prefetch_runner;
    };

    struct ThreadTask
    {
        using InfoPtr = MergeTreeReadTaskInfoPtr;

        ThreadTask(InfoPtr read_info_, MarkRanges ranges_, std::vector<MarkRanges> patches_ranges_, Priority priority_);

        ~ThreadTask()
        {
            if (readers_future && readers_future->valid())
                readers_future->wait();
        }

        bool isValidReadersFuture() const
        {
            return readers_future && readers_future->valid();
        }

        InfoPtr read_info;
        MarkRanges ranges;
        std::vector<MarkRanges> patches_ranges;
        Priority priority;
        std::unique_ptr<PrefetchedReaders> readers_future;
        /// Set by the asynchronous warmup job (deferred path) when skip-index filtering or reader
        /// creation throws. Rethrown by the reader thread in createTask() so the error is not lost
        /// (the synchronous path propagates such errors directly).
        std::exception_ptr warmup_exception;
    };

    struct TaskHolder
    {
        std::shared_ptr<ThreadTask> task;
        size_t thread_id = 0;
        bool operator<(const TaskHolder & other) const;
    };

    /// Shared so an in-flight asynchronous warmup job (deferred path) can co-own the task and keep
    /// it alive even after a reader thread takes it from per_thread_tasks via getTask().
    using ThreadTaskPtr = std::shared_ptr<ThreadTask>;
    using ThreadTasks = std::deque<ThreadTaskPtr>;
    using TasksPerThread = std::map<size_t, ThreadTasks>;
    using PartStatistics = std::vector<PartStatistic>;

    void fillPerPartStatistics();
    void fillPerThreadTasks(size_t threads, size_t sum_marks);

    /// Drains `prefetch_queue` under `mutex`. For the deferred (use_skip_indexes_on_data_read) path
    /// it does NOT schedule anything; it returns the tasks whose warmup jobs the caller must
    /// schedule via schedulePrefetchWarmupJobs() AFTER releasing `mutex`.
    std::vector<TaskHolder> startPrefetches();
    /// Schedules the per-part warmup jobs returned by startPrefetches(). MUST be called without
    /// holding `mutex`: the jobs acquire `mutex` to publish and run on the bounded
    /// `prefetch_threadpool`, so scheduling under the lock would deadlock.
    void schedulePrefetchWarmupJobs(const std::vector<TaskHolder> & tasks_to_warm_up);
    void createPrefetchedReadersForTask(ThreadTask & task);
    /// Does the expensive part of reader creation for a single task WITHOUT holding `mutex`:
    /// skip-index filtering (deferred path) and reader construction + prefetch issuing. Returns
    /// nullptr when the part is fully filtered out. The caller stores the result into
    /// `task.readers_future` (under `mutex` in the async path; already-locked in the sync path).
    /// `on_warmup_thread` is true when invoked from a prefetch_warmup_runner job, in which case
    /// prefetches are issued inline rather than scheduled onto the same threadpool.
    std::unique_ptr<PrefetchedReaders> buildReadersForTask(const ThreadTask & task, bool on_warmup_thread);
    std::function<void()> createPrefetchedTask(IMergeTreeReader * reader, Priority priority);

    MergeTreeReadTaskPtr stealTask(size_t thread, MergeTreeReadTask * previous_task);
    MergeTreeReadTaskPtr createTask(ThreadTask & thread_task, MergeTreeReadTask * previous_task);

    static std::string dumpTasks(const TasksPerThread & tasks);

    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    mutable std::mutex mutex;
    ThreadPool & prefetch_threadpool;

    PartStatistics per_part_statistics;
    TasksPerThread per_thread_tasks;
    std::priority_queue<TaskHolder> prefetch_queue; /// the smallest on top
    bool started_prefetches = false;
    LoggerPtr log;

    /// Runs per-task skip-index filtering + reader creation concurrently across parts on
    /// `prefetch_threadpool` (deferred path only). Declared after every member its jobs touch
    /// (mutex, prefetch_threadpool, per_thread_tasks, ...) so it is destroyed FIRST: its
    /// destructor drains all in-flight jobs while everything they reference is still alive.
    ThreadPoolCallbackRunnerLocal<void> prefetch_warmup_runner;

    /// A struct which allows to track max number of tasks which were in the
    /// threadpool simultaneously (similar to CurrentMetrics, but the result
    /// will be put to QueryLog).
    struct PrefetchIncrement : boost::noncopyable
    {
        explicit PrefetchIncrement(std::shared_ptr<AsyncReadCounters> counters_)
            : counters(counters_)
        {
            counters->total_prefetch_tasks.fetch_add(1, std::memory_order_relaxed);
            AsyncReadCounters::incrementAndUpdateMax(
                counters->current_parallel_prefetch_tasks, counters->max_parallel_prefetch_tasks);
        }

        ~PrefetchIncrement()
        {
            counters->current_parallel_prefetch_tasks.fetch_sub(1, std::memory_order_relaxed);
        }

        std::shared_ptr<AsyncReadCounters> counters;
    };
};

}
