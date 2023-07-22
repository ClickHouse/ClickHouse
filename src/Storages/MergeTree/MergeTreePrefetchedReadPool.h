#pragma once

#include <Common/ThreadPool_fwd.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
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
class MergeTreePrefetchedReadPool : public IMergeTreeReadPool, private WithContext
{
public:
    MergeTreePrefetchedReadPool(
        size_t threads,
        size_t sum_marks_,
        size_t min_marks_for_concurrent_read_,
        RangesInDataParts && parts_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const Names & column_names_,
        const Names & virtual_column_names_,
        size_t preferred_block_size_bytes_,
        const MergeTreeReaderSettings & reader_settings_,
        ContextPtr context_,
        bool use_uncompressed_cache_,
        bool is_remote_read_,
        const MergeTreeSettings & storage_settings_);

    MergeTreeReadTaskPtr getTask(size_t thread) override;

    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

    Block getHeader() const override { return header; }

    static bool checkReadMethodAllowed(LocalFSReadMethod method);
    static bool checkReadMethodAllowed(RemoteFSReadMethod method);

private:
    struct PartInfo;
    using PartInfoPtr = std::shared_ptr<PartInfo>;
    using PartsInfos = std::vector<PartInfoPtr>;
    using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;
    using ThreadTasks = std::deque<MergeTreeReadTaskPtr>;
    using ThreadsTasks = std::map<size_t, ThreadTasks>;

    std::future<MergeTreeReaderPtr> createPrefetchedReader(
        const IMergeTreeDataPart & data_part,
        const NamesAndTypesList & columns,
        const AlterConversionsPtr & alter_conversions,
        const MarkRanges & required_ranges,
        Priority priority) const;

    void createPrefetchedReaderForTask(MergeTreeReadTask & task) const;

    size_t getApproxSizeOfGranule(const IMergeTreeDataPart & part) const;

    PartsInfos getPartsInfos(const RangesInDataParts & parts, size_t preferred_block_size_bytes) const;

    ThreadsTasks createThreadsTasks(
        size_t threads,
        size_t sum_marks,
        size_t min_marks_for_concurrent_read) const;

    void startPrefetches() const;

    static std::string dumpTasks(const ThreadsTasks & tasks);

    Poco::Logger * log;

    Block header;
    MarkCache * mark_cache;
    UncompressedCache * uncompressed_cache;
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    size_t index_granularity_bytes;
    size_t fixed_index_granularity;

    StorageSnapshotPtr storage_snapshot;
    const Names column_names;
    const Names virtual_column_names;
    PrewhereInfoPtr prewhere_info;
    const ExpressionActionsSettings actions_settings;
    const MergeTreeReaderSettings reader_settings;
    RangesInDataParts parts_ranges;

    [[ maybe_unused ]] const bool is_remote_read;
    ThreadPool & prefetch_threadpool;

    PartsInfos parts_infos;

    ThreadsTasks threads_tasks;
    std::mutex mutex;

    struct TaskHolder
    {
        explicit TaskHolder(MergeTreeReadTask * task_, size_t thread_id_) : task(task_), thread_id(thread_id_) {}
        MergeTreeReadTask * task;
        size_t thread_id;
        bool operator <(const TaskHolder & other) const;
    };
    mutable std::priority_queue<TaskHolder> prefetch_queue; /// the smallest on top
    bool started_prefetches = false;

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
