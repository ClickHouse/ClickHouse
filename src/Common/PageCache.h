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

    /// Byte range in the file: [offset, offset + size).
    ///
    /// Note: for simplicity, PageCache doesn't do any interval-based lookup to handle partially
    /// overlapping ranges.
    /// E.g. if someone puts range [0, 100] to the cache, then someone else does getOrSet for
    /// range [0, 50], it'll be a cache miss, and the cache will end up with two ranges:
    /// [0, 100] and [0, 50].
    /// This is ok for correctness, but would be bad for performance if happens often.
    /// In practice this limitation causes no trouble as all users of page cache use aligned blocks
    /// of fixed size anyway (server setting page_cache_block_size).
    size_t offset = 0;
    size_t size = 0;

    UInt128 hash() const;
    std::string toString() const;
    size_t capacity() const { return path.capacity() + file_version.capacity(); }
};

class PageCacheCell
{
public:
    PageCacheKey key;

    size_t size() const { return m_size; }
    size_t capacity() const { return sizeof(*this) + key.capacity() + m_size; }
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
        return x.capacity();
    }
};

extern template class CacheBase<UInt128, PageCacheCell, UInt128TrivialHash, PageCacheWeightFunction>;

/// The key is hash of PageCacheKey.
///
/// Experimentally sharded, to reduce mutex contention. Contention seems unlikely to be a problem as
/// the blocks are pretty big (typically 1 MiB), and the main mutex is only locked during lookup,
/// not while downloading.
/// (If it turns out that having more than 1 shard never improves performance in practice, feel free
///  to simplify this by removing sharding.)
///
/// Implementation should be careful to always use MemoryTrackerBlockerInThread for all operations
/// that lock the mutex or allocate memory. Otherwise we'll can deadlock when MemoryTracker calls
/// autoResize.
class PageCache
{
private:
    using Base = CacheBase<UInt128, PageCacheCell, UInt128TrivialHash, PageCacheWeightFunction>;

    class alignas(std::hardware_destructive_interference_size) Shard : public Base
    {
    public:
        using Base::Base;

        void onEntryRemoval(size_t weight_loss, const MappedPtr & mapped_ptr) override;
    };

public:
    using Key = typename Base::Key;
    using Mapped = typename Base::Mapped;
    using MappedPtr = typename Base::MappedPtr;

    PageCache(
        std::chrono::milliseconds history_window_,
        const String & cache_policy,
        double size_ratio,
        size_t min_size_in_bytes_,
        size_t max_size_in_bytes_,
        double free_memory_ratio_,
        size_t num_shards);

    /// Get or insert a chunk for the given key.
    ///
    /// If detached_if_missing = true, and the key is not present in the cache, the returned chunk
    /// will be just a standalone PageCacheCell not connected to the cache.
    MappedPtr getOrSet(const PageCacheKey & key, bool detached_if_missing, bool inject_eviction, std::function<void(const MappedPtr &)> load);

    MappedPtr get(const PageCacheKey & key, bool inject_eviction);

    bool contains(const PageCacheKey & key, bool inject_eviction) const;

    /// Make the cache smaller by `memory_limit - memory_usage` bytes.
    /// Returns true if succeeded, false if cache size was reduced as much as possible but it wasn't enough.
    bool autoResize(Int64 memory_usage, size_t memory_limit);

    void clear();
    size_t sizeInBytes() const;
    size_t count() const;
    size_t maxSizeInBytes() const;

private:
    /// Cache size is automatically adjusted by background thread, within this range,
    /// targeting cache size (total_memory_limit * (1 - free_memory_ratio) - memory_used_excluding_cache).
    size_t min_size_in_bytes = 0;
    size_t max_size_in_bytes = 0;
    double free_memory_ratio = 1.;

    /// To avoid overreacting to brief drops in memory usage, we use peak memory usage over the last
    /// `history_window` milliseconds. It's calculated using this "sliding" (leapfrogging?) window.
    /// If history_window <= 0, there's no window and we just use current memory usage.
    std::mutex mutex;
    std::chrono::milliseconds history_window TSA_GUARDED_BY(mutex);
    std::array<size_t, 2> peak_memory_buckets TSA_GUARDED_BY(mutex) {0, 0};
    int64_t cur_bucket TSA_GUARDED_BY(mutex) = 0;

    std::vector<std::unique_ptr<Shard>> shards;

    size_t getShardIdx(UInt128 key) const
    {
        /// UInt128TrivialHash uses the lower 64 bits, we use the upper 64 bits.
        return key.items[UInt128::_impl::little(1)] % shards.size();
    }
};

using PageCachePtr = std::shared_ptr<PageCache>;

}
