#pragma once

#include <Processors/Formats/Impl/Parquet/ReadCommon.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <Processors/Formats/Impl/Parquet/Prefetcher.h>

#include <queue>
#include <deque>
#include <mutex>
#include <optional>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{
class Block;
}

namespace DB::Parquet
{

// TODO: check fields for false sharing, add cacheline padding as needed
// TODO: make sure userspace page cache read buffer supports readBigAt

/// Components of this parquet reader implementation:
///  * Prefetcher is responsible for coalescing nearby short reads into bigger reads.
///    It needs to know an approximate set of all needed ranges in advance, which we can produce
///    from parquet file metadata.
///  * SharedParsingThreadPool is shared can be shared across multiple parquet file readers belonging
///    to the same query, e.g. when doing `SELECT * FROM url('.../part_{0..999}.parquet')`.
///    Splits the memory and thread count budgets among the readers. Important because we want to
///    use much more memory per reader when reading one file than when reading 100 files in parallel.
///  * Reader implements parsing, type conversions, and filtering.
///    TODO: If it ends up split up, update this comment.
///  * ReadManager drives the Reader. Responsible for scheduling work to threads, thread safety,
///    limiting memory usage, and delivering output.

/// Each row group goes through these processing stages in sequence, possibly skipping some.
enum class ParsingStage
{
    NotStarted = 0,
    // TODO: Maybe add separate stage for reading `BloomFilterHeader`s, then read only the needed
    //       bloom filter blocks instead of the whole bloom filter.
    ApplyingBloomFilters,
    ApplyingPageIndex,
    ParsingPrewhereColumns,
    ApplyingPrewhere,
    ParsingRemainingColumns,
    Delivering,
    Deallocated,

    COUNT, // (not a stage)
};

struct Reader
{
    struct PrimitiveColumnInfo
    {
        /// Column index in parquet file. NOT index in primitive_columns array.
        size_t column_idx;
        bool is_nullable;
    };

    struct OutputColumnInfo
    {
        size_t primitive_start = 0;
        size_t primitive_end = 0;
        std::optional<size_t> index_in_output_block;
    };

    struct Page
    {
        parq::PageLocation location;
        size_t num_rows = 0;
        bool is_dictionary = false;

        /// Unlike all other prefetch requests, this one is created late and using splitAndEnqueueRange,
        /// when scheduling the column read task on the thread pool.
        Prefetcher::RequestHandle prefetch;
    };

    struct ColumnChunk
    {
        Prefetcher::RequestHandle bloom_filter_prefetch;
        Prefetcher::RequestHandle offset_index_prefetch;
        Prefetcher::RequestHandle column_index_prefetch;
        Prefetcher::RequestHandle data_prefetch; // covers all pages

        /// More fine-grained prefetch, if we decided to skip some pages based on filter.
        /// Empty if we're not skipping pages, and data_prefetch should be used instead.
        /// We preregister data_prefetch in Prefetch before we know page byte ranges
        /// (which come from offset_index_prefetch data), then split the range into smaller ranges
        /// if needed. If the whole data_prefetch is small and very close to other ranges (e.g. if
        /// column data is right next to offset index), Prefetcher may read it incidentally;
        /// then the `pages` prefetch ranges won't do any additional reading and will just point
        /// into the already-read bigger range.
        std::vector<Page> pages;

        /// Primitive column. ColumnNullable if PrimitiveColumnInfo says is_nullable.
        ColumnPtr column;

        /// If this primitive column is inside an array, this is the offsets for ColumnArray.
        /// If multiple nested arrays (Array(Array(...))), this lists them from inner to outer.
        /// Derived from parquet's repetition/definition levels.
        ///
        /// (In general, parquet file can also have multiple levels of nullables,
        /// e.g. `Nullable(Array(Nullable(Nullable(Array(...)))))`. But clickhouse only supports
        /// Nullable on primitive types. So we get rid of non-innermost nullables when processing
        /// repetition/definition levels. What's left is a maybe-nullable primitive `column`
        /// maybe nested inside some arrays.)
        std::vector<ColumnPtr> array_offsets;

        /// If true, the `column` is a ColumnLowCardinality that would become very big if converted
        /// to full column. We should split this row group (and hence `column`) into multiple
        /// chunks and expand+deliver them one by one to reduce memory usage.
        bool explosive_low_cardinality = false;
    };

    struct RowSet
    {
        bool all_pass = false;
        bool all_fail = false;
        IColumnFilter mask;
    };

    struct RowGroup
    {
        size_t row_group_idx; // in parquet file

        std::vector<ColumnChunk> primitive_columns;

        RowSet filter;
        ParsingStage stage;
        /// When this changes from nonzero to zero, the whole RowGroup experiences a synchronization
        /// point. Whoever makes such change is free to read and mutate any fields here without
        /// locking any mutexes.
        std::atomic<size_t> stage_tasks_remaining;

        std::vector<size_t> memory_usage_per_stage {};
    };

    ReadOptions options;
    std::shared_ptr<const KeyCondition> key_condition;
    Prefetcher prefetcher;

    parq::FileMetaData file_metadata;
    std::vector<RowGroup> row_groups;

    std::vector<PrimitiveColumnInfo> primitive_columns;
    std::vector<OutputColumnInfo> output_columns;
    std::vector<size_t> filter_columns;

    Reader(ReadBuffer * reader, const ReadOptions & options_, SharedParsingThreadPoolPtr thread_pool);

    void readFileMetaData();
    void prefilterAndInitRowGroups();

    /// Returns false if the row group was filtered out and should be skipped.
    bool applyBloomFilters(RowGroup & row_group);
    RowSet applyPageIndex(ColumnChunk & column, PrimitiveColumnInfo & column_info);
    /// Assigns `pages` if only a subset of pages need to be read.
    void determinePagesToRead(ColumnChunk & column, const RowSet & rows);
    void parsePrimitiveColumn(ColumnChunk & column, PrimitiveColumnInfo & column_info);
    ColumnPtr formOutputColumn(RowGroup & row_group, size_t output_column_idx, size_t first_row, size_t row_count);
    RowSet applyPrewhere(Block block);
};


class ReadManager
{
public:
    // I'd like to speak to the manager.
    ReadManager(ReadBuffer * reader_, const ReadOptions & options, SharedParsingThreadPoolPtr thread_pool_);

    ~ReadManager();

private:
    struct Task
    {
        ParsingStage stage;
        size_t row_group_idx;
        size_t column_idx;
        size_t memory_usage_estimate;
    };

    struct Stage
    {
        std::atomic<size_t> memory_usage {0};
        double memory_target_fraction;

        std::mutex mutex;
        std::queue<Task> tasks_to_schedule; // not scheduled on thread pool yet
    };

    SharedParsingThreadPoolPtr thread_pool;

    std::shared_ptr<ShutdownHelper> shutdown = std::make_shared<ShutdownHelper>();

    Reader reader;
    std::vector<Stage> stages;

    void scheduleTask(Task task);
    void runTask(Task task);
    void scheduleTasksIfNeeded(size_t stage_idx);
};


//TODO: delet dis
/// Simple RW spinlock with writer priority. Cachline padding recommended:
/// alignas(std::hardware_destructive_interference_size) RWSpinLockWriterFirst lock;
class RWSpinlockWriterFirst
{
public:
    void lock_shared()
    {
        lock_impl(1);
    }
    void unlock_shared()
    {
        Int64 n = val.fetch_sub(1, std::memory_order_release);
        chassert(n > 0);
    }
    void lock()
    {
        Int64 n = lock_impl(EXCLUSIVE);

        /// Wait for all shared locks to get unlocked.
        while (n > EXCLUSIVE)
        {
            std::this_thread::yield();
            n = val.load(std::memory_order_acquire);
            chassert(n >= EXCLUSIVE);
        }
    }
    void unlock()
    {
        Int64 n = val.fetch_sub(EXCLUSIVE, std::memory_order_release);
        chassert(n >= EXCLUSIVE);
    }
    bool try_lock_shared()
    {
        return try_lock_impl(1).second;
    }

private:
    static constexpr Int64 EXCLUSIVE = 1l << 42;
    /// val = [number of shared locks] + EXCLUSIVE*[number of exclusive candidate locks].
    std::atomic<Int64> val {0};

    /// For both shared and exclusive locks: if you managed to increase `val`
    /// from some value x < EXCLUSIVE to a value x + something, this means you got the lock.
    std::pair<Int64, bool> try_lock_impl(Int64 amount)
    {
        /// Speculatively add the value in hopes there's no contention.
        Int64 n = val.fetch_add(amount, std::memory_order_acquire);
        chassert(n >= 0);
        if (n < EXCLUSIVE)
            return {n + amount, true}; // fast path
        /// Oops, there's contention. Un-add and let the caller fall back to compare-and-swap loop.
        n = val.fetch_sub(amount, std::memory_order_relaxed) - amount;
        return {n, false};
    }
    Int64 lock_impl(Int64 amount)
    {
        Int64 n;
        bool locked;
        std::tie(n, locked) = try_lock_impl(amount);
        if (locked)
            return n;

        while (true)
        {
            chassert(n >= 0);
            if (n >= EXCLUSIVE)
            {
                std::this_thread::yield();
                n = val.load(std::memory_order_relaxed);
                continue;
            }
            if (val.compare_exchange_weak(n, n + amount, std::memory_order_acquire, std::memory_order_relaxed))
                return n + amount;
        }
    }
};

/// Relatively simple multiple-producer multiple-consumer ring buffer.
/// Probably faster than std::queue + std::mutex, probably slower than fancy complicated
/// implementations like folly::MPMCQueue (because of contention on atomics).
//(TODO)

}
