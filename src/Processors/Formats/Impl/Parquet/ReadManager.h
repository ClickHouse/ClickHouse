#pragma once

#include <Processors/Formats/Impl/Parquet/Reader.h>

namespace DB::Parquet
{

struct AtomicBitSet
{
    std::vector<std::atomic<UInt64>> a;

    void resize(size_t bits);

    bool set(size_t i, std::memory_order memory_order)
    {
        UInt64 mask = 1ul << (i & 63);
        UInt64 x = a[i >> 6].fetch_or(mask, memory_order);
        return (x & mask) == 0;
    }
    bool unset(size_t i, std::memory_order memory_order)
    {
        UInt64 mask = 1ul << (i & 63);
        UInt64 x = a[i >> 6].fetch_and(~mask, memory_order);
        return (x & mask) != 0;
    }

    std::optional<size_t> findFirst();
};

// I'd like to talk to the manager.
class ReadManager
{
public:
    Reader reader;

    /// To initialize ReadManager:
    ///  1. call manager.reader.prefetcher.init
    ///  2. call manager.reader.init
    ///  3. call manager.init
    /// (I'm trying this style because the usual pattern of passing-through lots of arguments through
    /// layers of constructors seems bad. This seems better but still not great, hopefully there's an
    /// even better way.)
    void init(FormatParserSharedResourcesPtr parser_shared_resources_, const std::optional<std::vector<size_t>> & buckets_to_read_);

    ~ReadManager();

    struct ReadResult
    {
        Chunk chunk;
        BlockMissingValues block_missing_values;
        size_t virtual_bytes_read = 0;
    };

    /// Not thread safe.
    ReadResult read();

    void cancel() noexcept;

private:
    using RowGroup = Reader::RowGroup;
    using RowSubgroup = Reader::RowSubgroup;
    using ColumnChunk = Reader::ColumnChunk;
    using ColumnSubchunk = Reader::ColumnSubchunk;
    using PrimitiveColumnInfo = Reader::PrimitiveColumnInfo;

    struct Task
    {
        ReadStage stage;
        size_t row_group_idx;
        size_t row_subgroup_idx = UINT64_MAX;
        size_t column_idx = UINT64_MAX;
        size_t cost_estimate_bytes = 0;

        struct Comparator
        {
            bool operator()(const Task & x, const Task & y) const
            {
                return std::make_tuple(x.row_group_idx, x.row_subgroup_idx) > std::make_tuple(y.row_group_idx, y.row_subgroup_idx);
            }
        };
    };

    struct Stage
    {
        std::atomic<size_t> memory_usage {0};
        /// Tasks that are either in thread pool's queue or executing.
        std::atomic<size_t> batches_in_progress {0};

        double memory_target_fraction = 1;

        /// We take advantage of the fact that each <row group, stage> pair can have at most one group
        /// of tasks in flight at a time. E.g. we create tasks to read columns in subgroup n, then
        /// wait for all of them to complete, then create tasks to read columns in subgroup n+1, etc.
        /// So each pair <row group, stage> is a sequence of groups of tasks, and their scheduling
        /// doesn't need any mutexes.
        AtomicBitSet schedulable_row_groups;
        std::vector<std::vector<Task>> row_group_tasks_to_schedule;
    };

    FormatParserSharedResourcesPtr parser_shared_resources;

    std::shared_ptr<ShutdownHelper> shutdown = std::make_shared<ShutdownHelper>();

    std::array<Stage, size_t(ReadStage::Deallocated)> stages;
    /// First row group that hasn't reached Deallocated stage.
    std::atomic<size_t> first_incomplete_row_group {0};

    std::mutex delivery_mutex;
    std::priority_queue<Task, std::vector<Task>, Task::Comparator> delivery_queue;
    std::condition_variable delivery_cv;
    std::exception_ptr exception;
    /// Nullopt means that ReadManager reads all row groups
    std::optional<std::unordered_set<UInt64>> row_groups_to_read;

    void scheduleTask(Task task, bool is_first_in_group, MemoryUsageDiff & diff, std::vector<Task> & out_tasks);
    void runTask(Task task, bool last_in_batch, MemoryUsageDiff & diff);
    void runBatchOfTasks(const std::vector<Task> & tasks) noexcept;
    void scheduleTasksIfNeeded(ReadStage stage_idx);
    void finishRowGroupStage(size_t row_group_idx, ReadStage stage, MemoryUsageDiff & diff);
    void finishRowSubgroupStage(size_t row_group_idx, size_t row_subgroup_idx, ReadStage stage, MemoryUsageDiff & diff);
    /// Free some memory ColumnChunk that's not needed after decoding is done in all row sugroups.
    /// Call sites should be careful to not call it from multiple threads in parallel.
    void clearColumnChunk(ColumnChunk & column, MemoryUsageDiff & diff);
    void clearRowSubgroup(RowSubgroup & row_subgroup, MemoryUsageDiff & diff);
    void setTasksToSchedule(size_t row_group_idx, ReadStage stage, std::vector<Task> add_tasks, MemoryUsageDiff & diff);
    void addTasksToReadColumns(size_t row_group_idx, size_t row_subgroup_idx, ReadStage stage, MemoryUsageDiff & diff);
    void advanceDeliveryPtrIfNeeded(size_t row_group_idx, MemoryUsageDiff & diff);
    void flushMemoryUsageDiff(MemoryUsageDiff && diff);
};

}
