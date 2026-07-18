#pragma once

#include <Common/threadPoolCallbackRunner.h>
#include <Formats/FormatSettings.h>

namespace DB
{
struct FormatParserSharedResources;
using FormatParserSharedResourcesPtr = std::shared_ptr<FormatParserSharedResources>;
struct FormatFilterInfo;
using FormatFilterInfoPtr = std::shared_ptr<FormatFilterInfo>;
}

namespace DB::Parquet
{

struct ReadOptions
{
    FormatSettings format;

    bool seekable_read = true;

    bool schema_inference_force_nullable = false;
    bool schema_inference_force_not_nullable = false;

    /// Use dictionary filter if the dictionary page is at most this size (the maximum is inclusive)
    /// and all values in the column chunk are dictionary-encoded. This takes precedence over bloom
    /// filter. 0 to disable.
    size_t dictionary_filter_limit_bytes = 0;

    size_t min_bytes_for_seek = 64 << 10;
    size_t bytes_per_read_task = 4 << 20;

    /// Don't use bloom filter for `x IN (...)` if the set `(...)` is has more than this many
    /// elements. There's no point using bloom filter for big sets because false positive
    /// probability becomes very high. E.g. if bloom filter has 1% false positive probability,
    /// searching for 100 elements would have 63% false positive probability.
    size_t bloom_filter_max_set_size = 100;
};

struct SharedResourcesExt
{
    size_t total_memory_low_watermark = 0;
    size_t total_memory_high_watermark = 0;

    struct Limits
    {
        size_t memory_low_watermark;
        size_t memory_high_watermark;
        size_t parsing_threads;
    };

    static Limits getLimitsPerReader(const FormatParserSharedResources & parser_shared_resources, double fraction);
};


/// Each column chunk goes through some subsequence of these stages, in order.
///
/// The scheduling of all this work (in ReadManager) is pretty complicated.
/// Some of the tasks apply to column chunk (e.g. reading bloom filter), some apply to part of
/// a column chunk ("column subchunk" we call it). Some stages need some per-row-group work after
/// finishing all per-column tasks (e.g. apply KeyCondition after reading bloom filters for all
/// columns).
///
/// Here's a slightly simplified dependency graph:
/// https://github.com/ClickHouse/ClickHouse/pull/82789#discussion_r2292203372
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

    OffsetIndex,
    ColumnData,

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
    /// Bit mask saying which ReadStage-s may have new tasks that can be scheduled to thread pool.
    UInt64 stages_to_schedule = 0;
    bool finalized = false;

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

    void scheduleAllStages()
    {
        stages_to_schedule = ~0ul;
    }
    void scheduleStage(ReadStage stage)
    {
        stages_to_schedule |= 1ul << size_t(stage);
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

    explicit operator bool() const { return alloc_stage != ReadStage::Deallocated; }

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


/// Live, cross-thread reservation of the memory used by dictionary-filter pruning. The decoded
/// dictionaries are charged to the `BloomFilterBlocksOrDictionary` stage counter in
/// `ReadManager::runTask` (and released in `clearColumnChunk`); the value sets built while evaluating
/// the row-group filter (`Reader::hashDictionaryValues`) are charged to the same counter through this
/// handle, for the lifetime of the lookup that holds them. Unlike a `MemoryUsageToken` - which records
/// its allocation in a per-batch `MemoryUsageDiff` that is only flushed to the stage counter when the
/// batch finishes - a value set is built and freed entirely within one batch, so a token would net to
/// zero at flush and never become visible to other threads. Charging the shared atomic directly keeps a
/// value set visible to every row group pruning in parallel for as long as it is alive, so their
/// combined footprint - across several dictionary-filtered columns in one row group and across several
/// row groups on different worker threads - stays within `input_format_parquet_memory_high_watermark`.
/// A reservation is taken before the value set is allocated, so real memory never exceeds it; if it
/// would push the total past the watermark the value set is not built and the row group is scanned in
/// full (fail-open, never a wrong result). A default-constructed reservation (or watermark 0) is
/// unbounded and charges nothing.
struct PruningMemoryReservation
{
    /// The `BloomFilterBlocksOrDictionary` stage's live memory counter (nullptr == unbounded).
    std::atomic<size_t> * stage_memory = nullptr;
    /// `input_format_parquet_memory_high_watermark` (0 == unbounded).
    size_t watermark = 0;
    /// This batch's pruning memory already charged to its `MemoryUsageDiff` but not yet flushed to
    /// `stage_memory` (the decoded dictionaries of the row group being pruned on this thread), so the
    /// reservation accounts for them even though other threads cannot see them yet.
    size_t in_flight = 0;

    /// Reserve `bytes` against the shared budget. Returns true and charges the counter if it fits;
    /// otherwise charges nothing and returns false (skip pruning). Unbounded reservations always fit.
    bool tryReserve(size_t bytes) const
    {
        if (stage_memory == nullptr || watermark == 0)
            return true;
        size_t before = stage_memory->fetch_add(bytes, std::memory_order_relaxed);
        if (before + bytes + in_flight > watermark)
        {
            stage_memory->fetch_sub(bytes, std::memory_order_relaxed);
            return false;
        }
        return true;
    }

    void release(size_t bytes) const
    {
        if (stage_memory != nullptr && watermark != 0 && bytes != 0)
            stage_memory->fetch_sub(bytes, std::memory_order_relaxed);
    }
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
