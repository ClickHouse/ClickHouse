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
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_);

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
        PrefetchedReaders(
            ThreadPool & pool, MergeTreeReadTask::Readers readers_, Priority priority_, MergeTreePrefetchedReadPool & read_prefetch);

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

        ThreadTask(InfoPtr read_info_, MarkRanges ranges_, Priority priority_)
            : read_info(std::move(read_info_)), ranges(std::move(ranges_)), priority(priority_)
        {
        }

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
        Priority priority;
        std::unique_ptr<PrefetchedReaders> readers_future;
    };

    struct TaskHolder
    {
        ThreadTask * task = nullptr;
        size_t thread_id = 0;
        bool operator<(const TaskHolder & other) const;
    };

    using ThreadTaskPtr = std::unique_ptr<ThreadTask>;
    using ThreadTasks = std::deque<ThreadTaskPtr>;
    using TasksPerThread = std::map<size_t, ThreadTasks>;
    using PartStatistics = std::vector<PartStatistic>;

    void fillPerPartStatistics();
    void fillPerThreadTasks(size_t threads, size_t sum_marks);

    void startPrefetches();
    void createPrefetchedReadersForTask(ThreadTask & task);
    std::function<void()> createPrefetchedTask(IMergeTreeReader * reader, Priority priority);

    MergeTreeReadTaskPtr stealTask(size_t thread, MergeTreeReadTask * previous_task);
    MergeTreeReadTaskPtr createTask(ThreadTask & thread_task, MergeTreeReadTask * previous_task);

    static std::string dumpTasks(const TasksPerThread & tasks);

    mutable std::mutex mutex;
    ThreadPool & prefetch_threadpool;

    PartStatistics per_part_statistics;
    TasksPerThread per_thread_tasks;
    std::priority_queue<TaskHolder> prefetch_queue; /// the smallest on top
    bool started_prefetches = false;
    LoggerPtr log;

    /// A struct which allows to track max number of tasks which were in the
    /// threadpool simultaneously (similar to CurrentMetrics, but the result
    /// will be put to QueryLog).
    struct PrefetchIncrement : boost::noncopyable
    {
        explicit PrefetchIncrement(std::shared_ptr<AsyncReadCounters> counters_)
            : counters(counters_)
        {
            std::lock_guard lock(counters->mutex);
            ++counters->total_prefetch_tasks;
            if (++counters->current_parallel_prefetch_tasks > counters->max_parallel_prefetch_tasks)
                counters->max_parallel_prefetch_tasks = counters->current_parallel_prefetch_tasks;

        }

        ~PrefetchIncrement()
        {
            std::lock_guard lock(counters->mutex);
            --counters->current_parallel_prefetch_tasks;
        }

        std::shared_ptr<AsyncReadCounters> counters;
    };
};

}
