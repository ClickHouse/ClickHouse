#pragma once

#include <Processors/Formats/Impl/Parquet/Reader.h>

namespace DB::Parquet
{

// I'd like to speak to the manager.
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
    void init(SharedParsingThreadPoolPtr thread_pool_);

    ~ReadManager();

    /// Not thread safe.
    Chunk read();

private:
    using RowGroup = Reader::RowGroup;
    using ColumnChunk = Reader::ColumnChunk;
    using PrimitiveColumnInfo = Reader::PrimitiveColumnInfo;

    struct Task
    {
        ParsingStage stage;
        size_t row_group_idx;
        /// Some stages have a Task per column, others have a Task per row group.
        /// The stages with Task per column may also do some per-row-group work after all per-column
        /// tasks complete, in finishRowGroupStage.
        size_t column_idx = UINT64_MAX;
    };

    struct Stage
    {
        std::atomic<size_t> memory_usage {0};
        double memory_target_fraction = 1;

        std::mutex mutex;
        /// Tasks not scheduled on thread pool yet.
        /// For Delivering stage, these tasks are picked up by read() instead of going to thread pool.
        std::queue<Task> tasks_to_schedule;
    };

    SharedParsingThreadPoolPtr thread_pool;

    std::shared_ptr<ShutdownHelper> shutdown = std::make_shared<ShutdownHelper>();

    std::array<Stage, size_t(ParsingStage::COUNT)> stages;

    std::mutex delivery_mutex;
    std::condition_variable delivery_cv;
    std::deque<size_t> deliverable_row_groups;
    size_t first_incomplete_row_group = 0;
    std::exception_ptr exception;

    void scheduleTask(Task task, MemoryUsageDiff * diff);
    void runTask(Task task);
    void scheduleTasksIfNeeded(ParsingStage stage_idx, std::unique_lock<std::mutex> &);
    void finishRowGroupStage(size_t row_group_idx, MemoryUsageDiff && diff);
    void flushMemoryUsageDiff(MemoryUsageDiff && diff);
};

}
