#include <Common/HeapProfiler.h>

#include <Common/AllocationTrace.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/memory.h>
#include <Common/logger_useful.h>
#include <Common/HashTable/Hash.h>

/// True during initialization and after destruction of HeapProfiler, to prevent recursive calls and
/// use-after-free.
static std::atomic_bool singleton_unavailable {false};

namespace DB
{

HeapProfiler::~HeapProfiler()
{
    /// Prevent operator delete from calling into HeapProfiler after it was destroyed on exit.
    singleton_unavailable.store(true);
}

static HeapProfiler * createInstance()
{
    singleton_unavailable.store(true);
    HeapProfiler * p = new HeapProfiler;
    singleton_unavailable.store(false);
    return p;
}

HeapProfiler & HeapProfiler::instance()
{
    static HeapProfiler * p = createInstance(); // leak on exit
    return *p;
}

void HeapProfiler::initialize(int64_t log_sample_rate_or_disabled, int64_t max_samples_or_auto, uint64_t physical_server_memory)
{
    chassert(log_sample_rate.load() == UINT64_MAX);
    if (log_sample_rate_or_disabled < 0)
        return;

    if (!Memory::allocatorSupportsUsableSize())
    {
        LOG_INFO(&Poco::Logger::get("HeapProfiler"), "Heap profiling is disabled because the allocator is not supported (not jemalloc, no malloc_usable_size()).");
        return;
    }

    uint64_t max_samples = uint64_t(max_samples_or_auto);
    if (max_samples_or_auto < 0)
    {
        /// Number of samples we'll store is around memory_usage / 2^log_sample_rate, but:
        ///  * it can be more if there's a lot of untouched virtual memory (not in RSS),
        ///  * it can be less if allocations are bigger than 2^log_sample_rate, so one sample
        ///    represents more memory,
        ///  * we want some extra room in the "hash tables" so collisions are not too frequent,
        ///  * there's random variance in number of samples per shard (rule of thumb is: the heaviest
        ///    shard will have 300%/sqrt(n) more samples than average, where n is the average number
        ///    of samples per shard).
        /// All these considerations are combined into one arbitrary constant 5.
        /// A sample takes ~0.5 KB of memory, so memory overhead of heap profiling is
        /// ~2.5 KB per 2^log_sample_rate of physical memory.
        max_samples = ((5 * physical_server_memory) >> log_sample_rate_or_disabled);
    }

    uint64_t samples_per_shard = 1; // must be nonzero and a power of 2
    while (samples_per_shard * shards.size() < max_samples)
        samples_per_shard *= 2;
    for (Shard & shard : shards)
    {
        std::unique_lock lock(shard.mutex);
        shard.hash_table.resize(samples_per_shard);
    }
    /// Enable sampling _after_ allocating the hash tables.
    log_sample_rate.store(log_sample_rate_or_disabled);
}

bool HeapProfiler::enabled() const
{
    return log_sample_rate.load() <= 62;
}

std::vector<HeapProfiler::Sample> HeapProfiler::dump()
{
    size_t samples_per_shard;
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        std::unique_lock lock(shards[0].mutex);
        samples_per_shard = shards[0].hash_table.size();
    }

    std::vector<Sample> res;
    res.reserve(samples_per_shard * shards.size());

    {
        DENY_ALLOCATIONS_IN_SCOPE;
        for (Shard & shard : shards)
        {
            std::unique_lock lock(shards[0].mutex);
            chassert(shard.hash_table.size() == samples_per_shard);
            res.insert(res.end(), shard.hash_table.begin(), shard.hash_table.end());
        }
    }

    return res;
}

void HeapProfiler::onAlloc(void * ptr, size_t requested_size)
{
    DENY_ALLOCATIONS_IN_SCOPE;

    int64_t weight;
    size_t shard_idx;
    size_t slot_idx;
    if (!locate(ptr, requested_size, &weight, &shard_idx, &slot_idx))
        return;

    StackTrace stack;
    stack.skipTopFrames(2); // omit the repetitive StackTrace::tryCapture() and onAlloc() frames
    Sample sample {.weight = weight, .ptr = ptr, .size = requested_size};
    if (CurrentThread::isInitialized())
    {
        sample.thread_id = CurrentThread::get().thread_id;
        std::string_view query_id = CurrentThread::getQueryId();
        sample.query_id_len = std::min(sample.query_id.size(), query_id.size());
        memcpy(sample.query_id.data(), query_id.data(), sample.query_id_len);
    }

    Shard & shard = shards[shard_idx];
    std::unique_lock lock(shard.mutex);
    slot_idx &= (shard.hash_table.size() - 1);
    shard.hash_table[slot_idx] = sample;
}

void HeapProfiler::onFree(void * ptr, size_t estimated_usable_size)
{
    DENY_ALLOCATIONS_IN_SCOPE;

    int64_t weight;
    size_t shard_idx;
    size_t slot_idx;
    if (!locate(ptr, estimated_usable_size, &weight, &shard_idx, &slot_idx))
        return;

    Shard & shard = shards[shard_idx];
    std::unique_lock lock(shard.mutex);
    slot_idx &= (shard.hash_table.size() - 1);
    Sample & s = shard.hash_table[slot_idx];
    if (s.ptr == ptr)
        s.weight = 0;
}

bool HeapProfiler::locate(void * ptr, size_t size, int64_t * out_weight, size_t * out_shard_idx, size_t * out_slot_idx)
{
    uint64_t log_sample_rate_ = log_sample_rate.load(std::memory_order_relaxed);
    if (log_sample_rate_ > 62)
        return false;

    /// The expected value of `weight` should be equal to `size / 2^log_sample_rate`.
    uint64_t whole_blocks = size >> log_sample_rate_;
    uint64_t block_mask = (1ul << log_sample_rate_) - 1;
    uint64_t partial_block = size & block_mask;
    /// Keep in mind that size may mismatch between alloc and free, so we can't e.g. include size
    /// in the hash. Increasing size can't decrease weight to zero.
    uint64_t hash = intHash64(uintptr_t(ptr));
    bool count_partial_block = (hash & block_mask) < partial_block;
    uint64_t weight = whole_blocks + uint64_t(count_partial_block);
    if (weight == 0)
        return false;

    /// Use bits of hash for different things.
    *out_weight = int64_t(weight << log_sample_rate_);
    hash >>= log_sample_rate_;
    *out_shard_idx = hash & ((1 << LOG_NUM_SHARDS) - 1);
    *out_slot_idx = hash >> LOG_NUM_SHARDS;

    return true;
}

}

void AllocationTrace::reportAllocToHeapProfiler(void * ptr, size_t requested_size) const
{
    if (!singleton_unavailable.load(std::memory_order_relaxed))
        DB::HeapProfiler::instance().onAlloc(ptr, requested_size);
}

void AllocationTrace::reportFreeToHeapProfiler(void * ptr, size_t estimated_usable_size) const
{
    if (!singleton_unavailable.load(std::memory_order_relaxed))
        DB::HeapProfiler::instance().onFree(ptr, estimated_usable_size);
}
