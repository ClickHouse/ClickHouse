#pragma once

#include <boost/intrusive/list.hpp>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include <Core/Types.h>
#include <Common/HashTable/HashMap.h>

/// "Userspace page cache"
/// A cache for contents of remote files.
/// Uses MADV_FREE to allow Linux to evict pages from our cache under memory pressure.
/// Typically takes up almost all of the available memory, similar to the actual page cache.
///
/// Intended for caching data retrieved from distributed cache, but can be used for other things too,
/// just replace FileChunkState with a discriminated union, or something, if needed.
///
/// There are two fixed-size units of caching here:
///  * OS pages, typically 4 KiB each.
///  * Page chunks, 2 MiB each (configurable with page_cache_block_size setting).
///
/// Each file is logically split into aligned 2 MiB blocks, which are mapped to page chunks inside the cache.
/// They are cached independently from each other.
///
/// Each page chunk has a contiguous 2 MiB buffer that can be pinned and directly used e.g. by ReadBuffers.
/// While pinned (by at least one PinnedPageChunk), the pages are not reclaimable by the OS.
///
/// Inside each page chunk, any subset of pages may be populated. Unpopulated pages may or not be
/// mapped to any physical RAM. We maintain a bitmask that keeps track of which pages are populated.
/// Pages become unpopulated if they're reclaimed by the OS (when the page chunk is not pinned),
/// or if we just never populate them in the first place (e.g. if a file is shorter than 2 MiB we
/// still create a 2 MiB page chunk, but use only a prefix of it).
///
/// There are two separate eviction mechanisms at play:
///  * LRU eviction of page chunks in PageCache.
///  * OS reclaiming pages on memory pressure. We have no control over the eviction policy.
///    It probably picks the pages in the same order in which they were marked with MADV_FREE, so
///    effectively in the same LRU order as our policy in PageCache.
/// When using PageCache in oversubscribed fashion, using all available memory and relying on OS eviction,
/// the PageCache's eviction policy mostly doesn't matter. It just needs to be similar enough to the OS's
/// policy that we rarely evict chunks with unevicted pages.
///
/// We mmap memory directly instead of using allocator because this enables:
///  * knowing how much RAM the cache is using, via /proc/self/smaps,
///  * MADV_HUGEPAGE (use transparent huge pages - this makes MADV_FREE 10x less slow),
///  * MAP_NORESERVE (don't reserve swap space - otherwise large mmaps usually fail),
///  * MADV_DONTDUMP (don't include in core dumps),
///  * page-aligned addresses without padding.
///
/// madvise(MADV_FREE) call is slow: ~6 GiB/s (doesn't scale with more threads). Enabling transparent
/// huge pages (MADV_HUGEPAGE) makes it 10x less slow, so we do that. That makes the physical RAM allocation
/// work at 2 MiB granularity instead of 4 KiB, so the cache becomes less suitable for small files.
/// If this turns out to be a problem, we may consider allowing different mmaps to have different flags,
/// some having no huge pages.
/// Note that we do our bookkeeping at small-page granularity even if huge pages are enabled.
///
/// It's unfortunate that Linux's MADV_FREE eviction doesn't use the two-list strategy like the real
/// page cache (IIUC, MADV_FREE puts the pages at the head of the inactive list, and they can never
/// get to the active list).
/// If this turns out to be a problem, we could make PageCache do chunk eviction based on observed
/// system memory usage, so that most eviction is done by us, and the MADV_FREE eviction kicks in
/// only as a last resort. Then we can make PageCache's eviction policy arbitrarily more sophisticated.

namespace DB
{

/// Hash of FileChunkAddress.
using PageCacheKey = UInt128;

/// Identifies a chunk of a file or object.
/// We assume that contents of such file/object don't change (without file_version changing), so
/// cache invalidation is needed.
struct FileChunkAddress
{
    /// Path, usually prefixed with storage system name and anything else needed to make it unique.
    /// E.g. "s3:<bucket>/<path>"
    std::string path;
    /// Optional string with ETag, or file modification time, or anything else.
    std::string file_version{};
    size_t offset = 0;

    PageCacheKey hash() const;

    std::string toString() const;
};

struct AtomicBitSet
{
    size_t n = 0;
    std::unique_ptr<std::atomic<UInt8>[]> v;

    AtomicBitSet();

    void init(size_t n);

    bool get(size_t i) const;
    bool any() const;
    /// These return true if the bit was changed, false if it already had the target value.
    /// (These methods are logically not const, but clang insists that I make them const, and
    ///  '#pragma clang diagnostic ignored' doesn't seem to work.)
    bool set(size_t i) const;
    bool set(size_t i, bool val) const;
    bool unset(size_t i) const;
    void unsetAll() const;
};

enum class PageChunkState : uint8_t
{
    /// Pages are not reclaimable by the OS, the buffer has correct contents.
    Stable,
    /// Pages are reclaimable by the OS, the buffer contents are altered (first bit of each page set to 1).
    Limbo,
};

/// (This is a separate struct just in case we want to use this cache for other things in future.
///  Then this struct would be the customization point, while the rest of PageChunk can stay unchanged.)
struct FileChunkState
{
    std::mutex download_mutex;

    void reset();
};

using PageChunkLRUListHook = boost::intrusive::list_base_hook<>;

/// Cache entry.
struct PageChunk : public PageChunkLRUListHook
{
    char * data;
    size_t size; // in bytes
    /// Page size for use in pages_populated and first_bit_of_each_page. Same as PageCache::pageSize().
    size_t page_size;

    /// Actual eviction granularity. Just for information. If huge pages are used, huge page size, otherwise page_size.
    size_t big_page_size;

    mutable FileChunkState state;

    AtomicBitSet pages_populated;

private:
    friend class PinnedPageChunk;
    friend class PageCache;

    /// If nullopt, the chunk is "detached", i.e. not associated with any key.
    /// Detached chunks may still be pinned. Chunk may get detached even while pinned, in particular when dropping cache.
    /// Protected by global_mutex.
    std::optional<PageCacheKey> key;

    /// Refcount for usage of this chunk. When zero, the pages are reclaimable by the OS, and
    /// the PageChunk itself is evictable (linked into PageCache::lru).
    std::atomic<size_t> pin_count {0};

    /// Bit mask containing the first bit of data from each page. Needed for the weird probing procedure when un-MADV_FREE-ing the pages.
    AtomicBitSet first_bit_of_each_page;

    /// Locked when changing pages_state, along with the corresponding expensive MADV_FREE/un-MADV_FREE operation.
    mutable std::mutex chunk_mutex;

    /// Normally pin_count == 0  <=>  state == PageChunkState::Limbo,
    ///          pin_count >  0  <=>  state == PageChunkState::Stable.
    /// This separate field is needed because of synchronization: pin_count is changed with global_mutex locked,
    /// this field is changed with chunk_mutex locked, and we never have to lock both mutexes at once.
    PageChunkState pages_state = PageChunkState::Stable;
};

class PageCache;

/// Handle for a cache entry. Neither the entry nor its pages can get evicted while there's at least one PinnedPageChunk pointing to it.
class PinnedPageChunk
{
public:
    const PageChunk * getChunk() const;

    /// Sets the bit in pages_populated. Returns true if it actually changed (i.e. was previously 0).
    bool markPagePopulated(size_t page_idx);

    /// Calls markPagePopulated() for pages 0..ceil(bytes/page_size).
    void markPrefixPopulated(size_t bytes);

    bool isPrefixPopulated(size_t bytes) const;

    PinnedPageChunk() = default;
    ~PinnedPageChunk() noexcept;

    PinnedPageChunk(PinnedPageChunk &&) noexcept;
    PinnedPageChunk & operator=(PinnedPageChunk &&) noexcept;

private:
    friend class PageCache;

    PageCache * cache = nullptr;
    PageChunk * chunk = nullptr;

    PinnedPageChunk(PageCache * cache_, PageChunk * chunk_) noexcept;
};

class PageCache
{
public:
    PageCache(size_t bytes_per_chunk, size_t bytes_per_mmap, size_t bytes_total, bool use_madv_free, bool use_huge_pages);
    ~PageCache();

    /// Get or insert a chunk for the given key.
    ///
    /// If detached_if_missing = true, and the key is not present in the cache, the returned chunk
    /// won't be associated with the key and will be evicted as soon as it's unpinned.
    /// It's like "get if exists, otherwise return null", but instead of null we return a usable
    /// temporary buffer, for convenience. Pinning and page eviction make the story more complicated:
    ///  * If the chunk for this key is pinned, we return it even if it's not fully populated
    ///    (because PageCache doesn't know what "fully populated" means).
    ///  * If the chunk exists, but some of its pages were evicted, we detach it. (Currently we only
    ///    check the first page here.)
    PinnedPageChunk getOrSet(PageCacheKey key, bool detached_if_missing, bool inject_eviction);

    /// OS page size, e.g. 4 KiB on x86, 4 KiB or 64 KiB on aarch64.
    ///
    /// If transparent huge pages are enabled, this is still the regular page size, and all our bookkeeping
    /// is still based on regular page size (e.g. pages_populated), because (a) it's cheap anyway,
    /// and (b) I'm not sure if Linux guarantees that MADV_FREE reclamation always happens at huge page
    /// granularity, and wouldn't want to rely on this even if it does.
    size_t pageSize() const;
    size_t chunkSize() const;
    size_t maxChunks() const;

    struct MemoryStats
    {
        /// How many bytes of actual RAM are used for the cache pages. Doesn't include metadata
        /// and overhead (e.g. PageChunk structs).
        size_t page_cache_rss = 0;
        /// Resident set size for the whole process, excluding any MADV_FREE pages (PageCache's or not).
        /// This can be used as a more useful memory usage number for clickhouse server, instead of RSS.
        /// Populated only if MADV_FREE is used, otherwise zero.
        std::optional<size_t> unreclaimable_rss;
    };

    /// Reads /proc/self/smaps, so not very fast.
    MemoryStats getResidentSetSize() const;

    /// Total length of memory ranges currently pinned by PinnedPageChunk-s, including unpopulated pages.
    size_t getPinnedSize() const;

    /// Clears the key -> chunk mapping. Frees memory (MADV_DONTNEED) of all chunks that are not pinned.
    /// Doesn't unmap any virtual memory. Detaches but doesn't free the pinned chunks.
    /// Locks the global mutex for the duration of the operation, which may block queries for hundreds of milliseconds.
    void dropCache();

private:
    friend class PinnedPageChunk;

    struct Mmap
    {
        void * ptr = nullptr;
        size_t size = 0;

        std::unique_ptr<PageChunk[]> chunks;
        size_t num_chunks = 0; // might be smaller than chunks_per_mmap_target because of alignment

        Mmap(Mmap &&) noexcept;
        Mmap(size_t bytes_per_page, size_t pages_per_chunk, size_t pages_per_big_page, size_t num_chunks, void * address_hint, bool use_huge_pages_);
        ~Mmap() noexcept;
    };

    size_t bytes_per_page;
    size_t pages_per_chunk;
    size_t chunks_per_mmap_target;
    size_t max_mmaps;
    size_t pages_per_big_page = 1; // if huge pages are used, huge_page_size/page_size, otherwise 1
    bool use_madv_free = true;
    bool use_huge_pages = true;

    mutable std::mutex global_mutex;

    pcg64 rng TSA_GUARDED_BY(global_mutex);

    std::vector<Mmap> mmaps TSA_GUARDED_BY(global_mutex);
    size_t total_chunks TSA_GUARDED_BY(global_mutex) = 0;

    /// All non-pinned chunks, including ones not assigned to any file. Least recently used is begin().
    boost::intrusive::list<PageChunk, boost::intrusive::base_hook<PageChunkLRUListHook>, boost::intrusive::constant_time_size<true>> lru TSA_GUARDED_BY(global_mutex);

    HashMap<PageCacheKey, PageChunk *> chunk_by_key TSA_GUARDED_BY(global_mutex);

    /// Get a usable chunk, doing eviction or allocation if needed.
    /// Caller is responsible for clearing pages_populated.
    PageChunk * getFreeChunk() TSA_REQUIRES(global_mutex);
    void addMmap() TSA_REQUIRES(global_mutex);
    void evictChunk(PageChunk * chunk) TSA_REQUIRES(global_mutex);

    void removeRef(PageChunk * chunk) noexcept;

    /// These may run in parallel with getFreeChunk(), so be very careful about which fields of the PageChunk we touch here.
    void sendChunkToLimbo(PageChunk * chunk, std::lock_guard<std::mutex> & /* chunk_mutex */) const noexcept;
    /// Returns {pages_restored, pages_evicted}.
    std::pair<size_t, size_t> restoreChunkFromLimbo(PageChunk * chunk, std::lock_guard<std::mutex> & /* chunk_mutex */) const noexcept;
};

using PageCachePtr = std::shared_ptr<PageCache>;

}
