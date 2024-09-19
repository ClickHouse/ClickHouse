#pragma once

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/thread_local_rng.h>
#include <IO/BufferWithOwnMemory.h>

/// "Userspace page cache"
/// A cache for contents of remote files.
/// Intended mainly for caching data retrieved from distributed cache or web disks.
/// Probably not useful when reading local files or when using file cache, the OS page cache works
/// well in those cases.
///
/// Similar to the OS page cache, we want this cache to use most of the available memory.
/// To that end, the cache size is periodically adjusted from background thread (MemoryWorker) based
/// on current memory usage.

namespace DB
{

/// Identifies a chunk of a file or object.
/// We assume that contents of such file/object don't change (without file_version changing), so
/// cache invalidation is not needed.
struct PageCacheKey
{
    /// Path, usually prefixed with storage system name and anything else needed to make it unique.
    /// E.g. "s3:<bucket>/<path>"
    std::string path;
    /// Optional string with ETag, or file modification time, or anything else.
    std::string file_version {};
    size_t offset = 0;
    size_t size = 0;

    UInt128 hash() const;
    std::string toString() const;
};

class PageCacheCell
{
public:
    PageCacheKey key;

    size_t size() const { return m_size; }
    const char * data() const { return m_data; }
    char * data() { return m_data; }

    ~PageCacheCell();
    PageCacheCell(const PageCacheCell &) = delete;
    PageCacheCell & operator=(const PageCacheCell &) = delete;

    PageCacheCell(PageCacheKey key_, bool temporary);

private:
    size_t m_size = 0;
    char * m_data = nullptr;
    bool m_temporary = false;
};

struct PageCacheWeightFunction
{
    size_t operator()(const PageCacheCell & x) const
    {
        return x.size();
    }
};

extern template class CacheBase<UInt128, PageCacheCell, UInt128TrivialHash, PageCacheWeightFunction>;

/// The key is hash of PageCacheKey.
/// Private inheritance because we have to add MemoryTrackerBlockerInThread to all operations that
/// lock the mutex, to avoid deadlocking if MemoryTracker calls autoResize().
class PageCache : private CacheBase<UInt128, PageCacheCell, UInt128TrivialHash, PageCacheWeightFunction>
{
public:
    using Base = CacheBase<UInt128, PageCacheCell, UInt128TrivialHash, PageCacheWeightFunction>;
    using Key = typename Base::Key;
    using Mapped = typename Base::Mapped;
    using MappedPtr = typename Base::MappedPtr;

    PageCache(size_t default_block_size_, const String & cache_policy, double size_ratio, size_t min_size_in_bytes_, size_t max_size_in_bytes_, double free_memory_ratio_);

    /// Get or insert a chunk for the given key.
    ///
    /// If detached_if_missing = true, and the key is not present in the cache, the returned chunk
    /// will be just a standalone PageCacheCell not connected to the cache.
    MappedPtr getOrSet(const PageCacheKey & key, bool detached_if_missing, bool inject_eviction, std::function<void(const MappedPtr &)> load);

    void autoResize(size_t memory_usage, size_t memory_limit);

    size_t defaultBlockSize() const { return default_block_size; }

    void clear();
    size_t sizeInBytes() const;
    size_t count() const;
    size_t maxSizeInBytes() const;

private:
    size_t default_block_size;

    /// Cache size is automatically adjusted by background thread, within this range,
    /// targeting cache size (total_memory_limit * (1 - free_memory_ratio) - memory_used_excluding_cache).
    size_t min_size_in_bytes = 0;
    size_t max_size_in_bytes = 0;
    double free_memory_ratio = 1.;

    //asdqwe
    std::array<size_t, 2> peak_memory_buckets {0, 0};
    int64_t cur_bucket = 0;

    void onRemoveOverflowWeightLoss(size_t /*weight_loss*/) override;
};

using PageCachePtr = std::shared_ptr<PageCache>;

}
