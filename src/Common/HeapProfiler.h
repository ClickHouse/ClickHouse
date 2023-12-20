#pragma once
#include <cstddef>
#include <base/defines.h>
#include <mutex>
#include <vector>
#include <Common/StackTrace.h>

/// HeapProfiler maintains an unbiased random sample of all currently alive memory allocations.
/// It can then be dumped periodically to make flamegraphs showing the breakdown of memory usage.
/// Similar to e.g. jemalloc's heap profiling.
///
/// This is separate from MemoryTracker's TraceType::MemorySample sampling because:
///  * Here, onAlloc() and onFree() calls must match exactly. Otherwise the sample will accumulate
///    garbage and won't be representative. So, HeapProfiler is exempt from untracked_memory_limit
///    and min_allocation_size/max_allocation_size.
///  * Here we sample by bytes rather than by allocation calls. This is important. E.g. suppose half
///    of the memory usage consists of 50 x 1 GB allocations; if we sample by allocations, we'll
///    almost certainly miss all 50 of them (feasible sample rates in production are on the
///    order of 1e-4).
///
/// Why keep a sample in memory instead of logging individual allocations/deallocations, then
/// doing a GROUP BY query to form a framegraph? Two reasons:
///  * The allocation size provided to onAlloc() and onFree() is just an estimate and doesn't necessarily
///    match between the calls. Typically the current memory usage is on the order of 0.005% of the
///    total allocated/deallocated amount since server startup. So if the alloc/free sizes mismatch
///    by 0.005% the heap profile will be off by 100% (whatever that means). So we have to remember the
///    size between alloc and free - that's storing samples in memory.
///    (Alternatively we could GROUP BY ptr at query time, but that would use lots of memory.)
///  * Dumping a heap profile every minute is ~5x less throughput than writing the same sampled
///    allocations/deallocations as they happen. (Estimate based on one workload, ymmv.)
///
/// Currently this class doesn't support changing sampling rate at runtime or per query,
/// for speed and simplicity (sample rate needs to match between allocation and deallocation).

namespace DB
{

constexpr size_t CACHE_LINE_ALIGNMENT = 128; // doesn't have to be exact

class alignas(CACHE_LINE_ALIGNMENT) HeapProfiler
{
public:
    struct Sample
    {
        int64_t weight = 0;

        void * ptr;
        size_t size;
        UInt64 thread_id = 0;
        size_t query_id_len = 0;
        std::array<char, 100> query_id;

        StackTrace stack;
    };

    static HeapProfiler & instance();

    /// Called at most once per process.
    void initialize(int64_t log_sample_rate_or_disabled, int64_t max_samples_or_auto, uint64_t physical_server_memory);

    bool enabled() const;

    /// The caller should ignore samples with weight == 0.
    std::vector<Sample> dump();

    /// Calls to onAlloc() and onFree() must always match, and estimated_usable_size must
    /// be >= requested_size.
    /// Except if allocatorSupportsUsableSize() is false, in which case MemoryProfiler is disabled.
    void onAlloc(void * ptr, size_t requested_size);
    void onFree(void * ptr, size_t estimated_usable_size);

    ~HeapProfiler();

private:
    static constexpr size_t LOG_NUM_SHARDS = 7;

    struct alignas(CACHE_LINE_ALIGNMENT) Shard
    {
        /// Never do allocations with this mutex locked or we'll deadlock.
        std::mutex mutex;

        /// No collision resolution. If we're adding a sample, and the slot is already occupied, we
        /// just overwrite it.
        std::vector<Sample> hash_table;
    };

    /// Probability of sampling an allocation is 2^-log_sample_rate.
    std::atomic<uint64_t> log_sample_rate {UINT64_MAX};
    char pad[CACHE_LINE_ALIGNMENT - sizeof(uint64_t)];

    /// Each sample goes into a pseudorandom shard to reduce lock contention.
    std::array<Shard, (1 << LOG_NUM_SHARDS)> shards;

    inline bool locate(void * ptr, size_t size, int64_t * out_weight, size_t * out_shard_idx, size_t * out_slot_idx);
};

}
