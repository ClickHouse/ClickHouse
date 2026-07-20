#include <Common/PageCache.h>

#include <sys/mman.h>
#include <Common/Allocator.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/formatReadable.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric PageCacheBytes;
    extern const Metric PageCacheCells;
}

namespace ProfileEvents
{
    extern const Event PageCacheHits;
    extern const Event PageCacheMisses;
    extern const Event PageCacheWeightLost;
    extern const Event PageCacheResized;
}

namespace DB
{

namespace ErrorCodes
{
}

template class CacheBase<UInt128, PageCacheCell, UInt128TrivialHash, PageCacheWeightFunction>;

UInt128 PageCacheKey::hash() const
{
    SipHash hash(offset);
    hash.update(size);
    hash.update(path.data(), path.size());
    hash.update("\0", 1);
    hash.update(file_version.data(), file_version.size());
    return hash.get128();
}

std::string PageCacheKey::toString() const
{
    return fmt::format("{}:{}:{}{}{}", path, offset, size, file_version.empty() ? "" : ":", file_version);
}

PageCache::PageCache(
    std::chrono::milliseconds history_window_,
    const String & cache_policy,
    double size_ratio,
    size_t min_size_in_bytes_,
    size_t max_size_in_bytes_,
    double free_memory_ratio_,
    size_t num_shards)
    : min_size_in_bytes(min_size_in_bytes_)
    , max_size_in_bytes(max_size_in_bytes_)
    , free_memory_ratio(free_memory_ratio_)
    , history_window(history_window_)
{
    num_shards = std::max(num_shards, 1ul);
    size_t bytes_per_shard = (min_size_in_bytes + num_shards - 1) / num_shards;
    for (size_t i = 0; i < num_shards; ++i)
    {
        shards.push_back(std::make_unique<Shard>(cache_policy,
            CurrentMetrics::PageCacheBytes, CurrentMetrics::PageCacheCells,
            bytes_per_shard, Base::NO_MAX_COUNT, size_ratio));
    }
}

PageCache::MappedPtr PageCache::getOrSet(const PageCacheKey & key, bool detached_if_missing, bool inject_eviction, std::function<void(const MappedPtr &)> load)
{
    /// Prevent MemoryTracker from calling autoResize while we may be holding the mutex.
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);

    Key key_hash = key.hash();

    Shard & shard = *shards[getShardIdx(key_hash)];

    if (inject_eviction && thread_local_rng() % 10 == 0)
        shard.remove(key_hash);

    MappedPtr result;
    bool miss = false;
    if (detached_if_missing)
    {
        result = shard.get(key_hash);
        if (!result)
        {
            blocker.reset(); // allow throwing out-of-memory exception when allocating or loading cell

            miss = true;
            result = std::make_shared<PageCacheCell>(key, /*temporary*/ true);
            load(result);
        }
    }
    else
    {
        std::tie(result, miss) = shard.getOrSet(key_hash, [&]() -> MappedPtr
        {
            /// At this point CacheBase is not holding the mutex, so it's ok to let MemoryTracker
            /// call autoResize.
            blocker.reset();

            MappedPtr cell;
            try
            {
                cell = std::make_shared<PageCacheCell>(key, /*temporary*/ false);
                load(cell);
            }
            catch (...)
            {
                blocker = MemoryTrackerBlockerInThread(VariableContext::Global);
                throw;
            }

            blocker = MemoryTrackerBlockerInThread(VariableContext::Global);
            return cell;
        });
    }
    chassert(result);

    if (miss)
        ProfileEvents::increment(ProfileEvents::PageCacheMisses);
    else
        ProfileEvents::increment(ProfileEvents::PageCacheHits);

    return result;
}

bool PageCache::contains(const PageCacheKey & key, bool inject_eviction) const
{
    /// Avoid deadlock if MemoryTracker calls PageCache::autoResize.
    /// (If you're here because it turned out that CacheBase::contains actually needs to allocate,
    ///  just replace this with MemoryTrackerBlockerInThread, like in the other methods here.)
    DENY_ALLOCATIONS_IN_SCOPE;

    if (inject_eviction && thread_local_rng() % 10 == 0)
        return false;
    Key key_hash = key.hash();
    const Shard & shard = *shards[getShardIdx(key_hash)];
    return shard.contains(key_hash);
}

void PageCache::Shard::onEntryRemoval(const size_t weight_loss, const MappedPtr & mapped_ptr)
{
    ProfileEvents::increment(ProfileEvents::PageCacheWeightLost, weight_loss);
    UNUSED(mapped_ptr);
}

void PageCache::autoResize(Int64 memory_usage_signed, size_t memory_limit)
{
    /// Avoid recursion when called from MemoryTracker.
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);

    size_t cache_size = sizeInBytes();
    size_t memory_usage = size_t(std::max(memory_usage_signed, Int64(0)));

    size_t peak;
    {
        std::lock_guard lock(mutex);
        size_t usage_excluding_cache = memory_usage - std::min(cache_size, memory_usage);

        if (history_window.count() <= 0)
        {
            peak = usage_excluding_cache;
        }
        else
        {
            int64_t now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            int64_t bucket = now / history_window.count();
            if (bucket > cur_bucket + 1)
                peak_memory_buckets = {0, 0};
            else if (bucket == cur_bucket + 1)
                peak_memory_buckets = {0, peak_memory_buckets[0]};
            cur_bucket = bucket;
            peak_memory_buckets[0] = std::max(peak_memory_buckets[0], usage_excluding_cache);
            peak = std::max(peak_memory_buckets[0], peak_memory_buckets[1]);
        }
    }

    size_t reduced_limit = size_t(memory_limit * (1. - std::min(free_memory_ratio, 1.)));
    size_t target_size = reduced_limit - std::min(peak, reduced_limit);
    target_size = std::clamp(target_size, min_size_in_bytes, max_size_in_bytes);

    size_t size_per_shard = (target_size + shards.size() - 1) / shards.size();
    for (const auto & shard : shards)
        shard->setMaxSizeInBytes(size_per_shard);

    ProfileEvents::increment(ProfileEvents::PageCacheResized);
}

void PageCache::clear()
{
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);
    for (const auto & shard : shards)
        shard->clear();
}

size_t PageCache::sizeInBytes() const
{
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);
    size_t sum = 0;
    for (const auto & shard : shards)
        sum += shard->sizeInBytes();
    return sum;
}

size_t PageCache::count() const
{
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);
    size_t sum = 0;
    for (const auto & shard : shards)
        sum += shard->count();
    return sum;
}

size_t PageCache::maxSizeInBytes() const
{
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);
    size_t sum = 0;
    for (const auto & shard : shards)
        sum += shard->maxSizeInBytes();
    return sum;
}

PageCacheCell::PageCacheCell(PageCacheKey key_, bool temporary) : key(std::move(key_)), m_size(key.size), m_temporary(temporary)
{
    /// Don't attribute page cache memory to the query that happened to allocate it.
    std::optional<MemoryTrackerBlockerInThread> blocker;
    if (!m_temporary)
        blocker.emplace();

    /// Allow throwing out-of-memory exceptions from here.
    m_data = reinterpret_cast<char *>(Allocator<false>().alloc(m_size));
}

PageCacheCell::~PageCacheCell()
{
    std::optional<MemoryTrackerBlockerInThread> blocker;
    if (!m_temporary)
        blocker.emplace();
    Allocator<false>().free(m_data, m_size);
}

}
