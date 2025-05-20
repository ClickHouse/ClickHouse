#pragma once

#include <condition_variable>
#include <shared_mutex>
#include <Common/futex.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

namespace DB::Parquet
{

struct ReadOptions
{
    /// TODO [parquet]: make sure all of these are randomized in tests; writer settings too.
    bool use_row_group_min_max = true;
    bool use_bloom_filter = true;
    bool use_page_min_max = true;
    bool always_use_offset_index = true;

    bool seekable_read = true;

    bool case_insensitive_column_matching = false;
    bool schema_inference_force_nullable = false;
    bool schema_inference_force_not_nullable = false;
    bool null_as_default = true;

    bool fuzz = false; // TODO [parquet]: Use it.

    /// Use dictionary filter if dictionary page is smaller than this (and all values in the column
    /// chunk are dictionary-encoded). This takes precedence over bloom filter. 0 to disable.
    size_t dictionary_filter_limit_bytes = 4 << 10;

    size_t min_bytes_for_seek = 64 << 10;
    size_t bytes_per_read_task = 4 << 20;

    size_t preferred_block_size_bytes = DEFAULT_BLOCK_SIZE * 256;
    size_t max_block_size = DEFAULT_BLOCK_SIZE;
};


/// TODO [parquet]: Move this to a shared file and reconcile with https://github.com/ClickHouse/ClickHouse/pull/66253/files , probably make it opaque and created by FormatFactory or something, or contains an opaque shared_ptr lazy-initialized by the format
struct SharedParsingThreadPool
{
    ThreadPoolCallbackRunnerFast io_runner;
    ThreadPoolCallbackRunnerFast parsing_runner;

    size_t total_memory_low_watermark = 0;
    size_t total_memory_high_watermark = 0;
    std::atomic<size_t> num_readers {0};

    struct Limits
    {
        size_t memory_low_watermark;
        size_t memory_high_watermark;
        size_t parsing_threads;
    };

    Limits getLimitsPerReader(double fraction) const
    {
        size_t n = num_readers.load(std::memory_order_relaxed);
        fraction /= std::max(n, size_t(1));
        return Limits {
            .memory_low_watermark = size_t(total_memory_low_watermark * fraction),
            .memory_high_watermark = size_t(total_memory_high_watermark * fraction),
            .parsing_threads = std::max(size_t(parsing_runner.getMaxThreads() * fraction + .5), size_t(1))};
    }
};

using SharedParsingThreadPoolPtr = std::shared_ptr<SharedParsingThreadPool>;


/// Each column chunk goes through some subsequence of these stages, in order.
///
/// The scheduling of all this work (in ReadManager) is pretty complicated.
/// Some of the tasks apply to column chunk (e.g. reading bloom filter), some apply to part of
/// a column chunk ("column subchunk" we call it). Some stages need some per-row-group work after
/// finishing all per-column tasks (e.g. apply KeyCondition after reading bloom filters for all
/// columns).
///
/// Here's a slightly simplified dependency graph:
/// https://github.com/user-attachments/assets/57213f9c-1588-406c-980e-ff0a9ab56e95
/// (if you need to edit this diagram, load this into excalidraw:
///  https://pastila.nl/?cafebabe/5f32c6546f4797c537707535c515f2c3#Fp02Ps7p2hRahC0B5cK+TQ== )
///
/// An important role of this enum is to separately control parallelism of different stages.
/// E.g. typically column index is small, and we can read it in lots of columns and row groups
/// in parallel (especially useful if we're reading over network and are latency-bound).
/// But main data is often big enough that we can't afford enough memory to read many row groups in
/// parallel. We'd like the parallelism to automatically scale based on memory usage.
/// But also we don't want to get into a situation where e.g. most of the memory budget is used by
/// column indexes and there's not enough left to read main data for a few row groups in parallel.
/// To solve these two problems at once, we do memory accounting separately for each stage, with
/// separate memory budget for each stage (see ReadManager::Stage).
/// Memory is attributed to the stage that allocated it. E.g. ReadManager::read() (Deliver stage)
/// may release a column that was allocated by PrewhereData stage, reducing PrewhereData's memory
/// usage and potentially kicking off more PrewhereData read tasks.
enum class ReadStage
{
    NotStarted = 0,
    BloomFilterHeader,
    BloomFilterBlocksOrDictionary,
    ColumnIndexAndOffsetIndex,
    PrewhereOffsetIndex,
    PrewhereData,
    MainOffsetIndex, // "main" means columns that are not in prewhere
    MainData,
    Deliver,
    Deallocated,
};

/// Subsequence of ReadStage-s relevant to a whole RowGroup.
/// Each such stage transition is a barrier synchronizing all columns of the row group.
/// E.g. bloom filters have two stages in ReadStage but one stage here because the two stages
/// (read header, then read blocks) need to happen sequentially within each column; then, after all
/// columns finish both stages, some row-group-level work needs to happen (applying KeyCondition
/// using all columns' bloom filters at once) before any column can proceed to the next stage.
enum class RowGroupReadStage
{
    NotStarted = 0,
    BloomAndDictionaryFilters,
    ColumnIndex,
    Subgroups,
    Deallocated,
};

enum class RowSubgroupReadStage
{
    NotStarted = 0,
    Prewhere,
    MainColumns,
    Deliver,
    Deallocated,
};


/// We track approximate current memory usage per ReadStage that allocated the memory (*).
/// This struct aggregates how much memory was allocated by some operation.
/// ReadManager then uses it to update per-stage memory usage std::atomic counters.
/// (We do this instead of updating the std::atomics directly to reduce contention on the atomics.
///  I haven't checked whether this makes a difference.)
///
/// (*) This is to have a separate memory limit on each stage to automatically get higher parallelism
/// for stages that use little memory (e.g. prefetch small bloom filters and indexes for lots of row
/// groups in parallel, but read large column data for few row groups to not run out of memory).
/// TODO [parquet]: Try using thread-locals instead of manually error-pronely passing this everywhere.
struct MemoryUsageDiff
{
    ReadStage cur_stage;
    std::array<ssize_t, size_t(ReadStage::Deallocated)> by_stage {};
    bool finalized = false;
    /// True if we may have unblocked some tasks by means other then freeing memory
    /// (specifically, we advanced first_incomplete_row_group or delivery_ptr).
    bool retry_scheduling_for_all_stages = false;
    bool retry_scheduling_for_cur_stage = false;

    explicit MemoryUsageDiff(ReadStage cur_stage_) : cur_stage(cur_stage_) {}
    MemoryUsageDiff() = delete;
    MemoryUsageDiff(const MemoryUsageDiff &) = delete;
    MemoryUsageDiff & operator=(const MemoryUsageDiff &) = delete;

    ~MemoryUsageDiff()
    {
        chassert(finalized || std::uncaught_exceptions() > 0);
    }

    void allocated(size_t amount)
    {
        chassert(cur_stage > ReadStage::NotStarted);
        chassert(cur_stage < ReadStage::Deliver);
        chassert(!finalized);
        by_stage.at(size_t(cur_stage)) += ssize_t(amount);
    }
    void deallocated(size_t amount, ReadStage stage)
    {
        chassert(!finalized);
        by_stage.at(size_t(stage)) -= ssize_t(amount);
    }
};

/// Remembers the ReadStage and size of a memory allocation.
/// Not RAII, you have to call reset to update the stat.
class MemoryUsageToken
{
public:
    MemoryUsageToken() = default;
    MemoryUsageToken(size_t val_, MemoryUsageDiff * diff)
        : alloc_stage(diff->cur_stage), val(val_)
    {
        diff->allocated(val);
    }
    MemoryUsageToken(MemoryUsageToken && rhs) noexcept
    {
        *this = std::move(rhs);
    }
    MemoryUsageToken & operator=(MemoryUsageToken && rhs) noexcept
    {
        chassert(!val);
        alloc_stage = std::exchange(rhs.alloc_stage, ReadStage::Deallocated);
        val = std::exchange(rhs.val, 0);
        return *this;
    }

    operator bool() const { return alloc_stage != ReadStage::Deallocated; }

    void reset(MemoryUsageDiff * diff)
    {
        if (val)
            diff->deallocated(val, alloc_stage);
        val = 0;
        alloc_stage = ReadStage::Deallocated;
    }
    void add(size_t amount, MemoryUsageDiff * diff)
    {
        chassert(diff->cur_stage == alloc_stage);
        diff->allocated(amount);
        val += amount;
    }

private:
    ReadStage alloc_stage = ReadStage::Deallocated;
    size_t val = 0;
};


#ifdef OS_LINUX

class CompletionNotification
{
private:
    enum State : UInt32
    {
        EMPTY,
        WAITING,
        NOTIFIED,
    };

    std::atomic<UInt32> val {0};

public:
    bool check() const;
    void wait();
    void notify();
};

#else

class CompletionNotification
{
private:
    std::promise<void> promise;
    std::future<void> future = promise.get_future();
    std::atomic<bool> notified {false};

public:
    bool check() const;
    void wait();
    void notify();
};

#endif

}
