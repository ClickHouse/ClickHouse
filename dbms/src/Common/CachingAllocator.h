#pragma once

#include <atomic>
#include <mutex>
#include <list>
#include <memory>
#include <random>
#include <pcg_random.hpp>
#include <unordered_map>
#include <sys/mman.h>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/noncopyable.hpp>
#include <ext/scope_guard.h>

#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Common/formatReadable.h>

/// Old Darwin builds lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

namespace DB::ErrorCodes
{
extern const int CANNOT_ALLOCATE_MEMORY;
extern const int CANNOT_MUNMAP;
}

struct CachingAllocatorStats
{
    size_t total_chunks_size {0};
    size_t total_allocated_size {0};
    size_t total_size_currently_initialized {0};
    size_t total_size_in_use {0};

    size_t num_chunks {0};
    size_t num_regions {0};
    size_t num_free_regions {0};
    size_t num_regions_in_use {0};
    size_t num_keyed_regions {0};

    size_t hits {0};
    size_t concurrent_hits {0};
    size_t misses {0};

    size_t allocations {0};
    size_t allocated_bytes {0};
    size_t evictions {0};
    size_t evicted_bytes {0};
    size_t secondary_evictions {0};
};

/**
 * Thread-safe caching allocator interface.
 * Uses mmap as an [de]allocator tool.
 *
 *
 * Motivation:
 * <ul>
 * <li>Cache has a specific memory usage limit (and we want it to include allocator fragmentation overhead).</li>
 * <li>Memory fragmentation can be bridged over using cache eviction.</li>
 * <li>There is no sense for reclaimed memory regions to be cached internally by usual allocator (malloc/new)
 *   as we implement our own routines.</li>
 * <li>Direct memory allocation serves for debugging purposes: placing mmap'd memory far from malloc'd facilitates
 *   revealing memory stomping bugs.</li>
 * </ul>
 *
 * @note Cache is not NUMA friendly.
 *
 * Performance: few million ops/sec from single thread, less in case of concurrency.
 * Fragmentation is usually less than 10%.
 */
template <typename TKey, typename TMapped, typename THash = std::hash<TKey>>
class CachingAllocator : private boost::noncopyable
{
private:
    /**
     * We will allocate memory in chunks of at least that size.
     * 64 MB makes mmap overhead comparable to memory throughput.
     */
    static constexpr size_t min_chunk_size = 64 * 1024 * 1024;

    static constexpr size_t page_size = 4096;

    /// Sizes and addresses of allocated memory will be aligned to this boundary.
    static constexpr size_t ptrs_alignment = 16;

    struct MemoryRegion;

public:
    using Key    = TKey;
    using Mapped = TMapped;
    using Hash   = THash;

    using MemoryRegionPtr = std::shared_ptr<MemoryRegion>;

    explicit CachingAllocator(size_t max_total_size_) noexcept: max_total_size(max_total_size_) {}

    ~CachingAllocator() noexcept
    {
        std::lock_guard cache_lock(mutex);

        allocated_regions.clear();
        unused_allocated_regions.clear();
        free_regions.clear();

        all_regions.clear_and_dispose(region_metadata_disposer);
    }

    inline size_t getSizeInUse() const
    {
        std::lock_guard lock(mutex);
        return total_size_in_use;
    }

    inline size_t getUsedRegionsCount() const
    {
        std::lock_guard lock(mutex);
        return allocated_regions.size();
    }

    inline size_t reset()
    {
        std::lock_guard lock(mutex);

        insertion_attempts.clear();
        chunks.clear();

        free_regions.clear();
        allocated_regions.clear();
        unused_allocated_regions.clear();

        all_regions.clear_and_dispose(region_metadata_disposer);

        total_chunks_size = 0;
        total_allocated_size = 0;
        total_size_in_use = 0;

        total_size_currently_initialized = 0;

        hits = 0;
        concurrent_hits  = 0;
        misses = 0;

        allocations  = 0;
        allocated_bytes = 0;
        evictions = 0;
        evicted_bytes = 0;
        secondary_evictions = 0;
    }

    inline MemoryRegionPtr get(const Key& key)
    {
        std::lock_guard lock(mutex);

        InsertionAttemptDisposer disposer;
        InsertionAttempt * attempt;

        return getImpl(key, disposer, attempt);
    }

    /**
     *  If the value for the key is in the cache, returns it.
     *
     * Otherwise, calls \c get_size to obtain required size of storage for the key, then allocates storage and calls
     *  \c initialize for necessary initialization before data from cache could be used.
     *
     * @note Only one of several concurrent threads calling this method will call \c get_size or \c initialize functors.
     *      Others will wait for that call to complete and will use its result (this helps to prevent cache stampede).
     *
     * @note Exceptions that occur in callback functors (\c get_size and \c initialize) will be propagated to the caller.
     *      Another thread from the set of concurrent threads will then try to call its callbacks et cetera.
     *
     * @note During insertion, each key is locked - to avoid parallel initialization of regions for same key.
     *
     * @param key Key that is used to find the target value.
     *
     * @param get_size Callback functor that is called if value with a given \c key was neither found in cache
     *      nor produced by any of other threads.
     *      Functor is assumed to be invokable with no arguments and return a size_t representing
     *      the size needed to be allocated.
     *
     * @param initialize Callback function that is called if
     *      <ul>
     *      <li>\c get_size is called and doesn't throw.</li>
     *      <li>Allocator did not fail to allocate another memory region.</li>
     *      </ul>
     *      Functor is assumed to be invokable with arguments (<tt>void * ptr, Payload& payload</tt>).
     *      TODO What must it do?
     *
     * @return Cached value inside a \c MemoryRegion wrapper (it prevents the cache entry from eviction).
     * @return Bool indicating whether the value was produced during the call. <br>
     *      False on cache hit (including a concurrent cache hit), true on cache miss).
     */
    template <typename GetSize, typename Init>
    inline std::pair<MemoryRegionPtr, bool> getOrSet(const Key & key, GetSize && get_size, Init && initialize)
    {
        InsertionAttemptDisposer disposer;
        InsertionAttempt * attempt;

        MemoryRegionPtr out = getImpl(key, disposer, attempt);

        if (out)
            return {out, false}; // value was found in the cache.

        /// No try-catch here because it is not needed.
        size_t size = get_size();

        RegionMetadata * region;

        {
            std::lock_guard cache_lock(mutex);
            region = allocate(size);
        }

        /// Cannot allocate memory.
        if (!region) return {};

        region->key = key;

        {
            total_size_currently_initialized += size;
            SCOPE_EXIT({ total_size_currently_initialized -= size; });

            try
            {
                initialize(region->ptr, region->payload);
            }
            catch (...)
            {
                {
                    std::lock_guard cache_lock(mutex);
                    freeAndCoalesce(*region);
                }

                throw;
            }
        }

        std::lock_guard cache_lock(mutex);

        try
        {
            attempt->value = std::make_shared<MemoryRegion>(*this, *region);

            /// Insert the new value only if the attempt is still present in \c insertion_attempts.
            /// (may be absent because of a concurrent reset() call).
            auto attempt_it = insertion_attempts.find(key);

            if (insertion_attempts.end() != attempt_it && attempt_it->second.get() == attempt)
                allocated_regions.insert(*region);

            if (!attempt->is_disposed)
                disposer.dispose();

            return {attempt->value, true};
        }
        catch (...)
        {
            if (region->TAllocatedRegionHook::is_linked())
                allocated_regions.erase(allocated_regions.iterator_to(*region));

            freeAndCoalesce(*region);

            throw;
        }
    }

    /**
     * @note Metadata is allocated by a usual allocator (malloc/new) so its memory usage is not accounted.
     */
    [[nodiscard]] inline CachingAllocatorStats getStatistics() const noexcept
    {
        std::lock_guard cache_lock(mutex);

        CachingAllocatorStats out = {};

        out.total_chunks_size = total_chunks_size;
        out.total_allocated_size = total_allocated_size;
        out.total_size_currently_initialized = total_size_currently_initialized.load(std::memory_order_relaxed);
        out.total_size_in_use = total_size_in_use;

        out.num_chunks = chunks.size();
        out.num_regions = all_regions.size();
        out.num_free_regions = free_regions.size();
        out.num_regions_in_use = all_regions.size() - free_regions.size() - unused_allocated_regions.size();
        out.num_keyed_regions = allocated_regions.size();

        out.hits = hits.load(std::memory_order_relaxed);
        out.concurrent_hits = concurrent_hits.load(std::memory_order_relaxed);
        out.misses = misses.load(std::memory_order_relaxed);

        out.allocations = allocations;
        out.allocated_bytes = allocated_bytes;
        out.evictions = evictions;
        out.evicted_bytes = evicted_bytes;
        out.secondary_evictions = secondary_evictions;

        return out;
    }

private:
    /**
     * Each memory region can be:
     * - Free: not holding any data.
     * - Allocated, but unused (can be evicted).
     * - Allocated and used (addressed by \c key, can't be evicted).
     */

    struct UnusedRegionTag;
    struct RegionTag;
    struct FreeRegionTag;
    struct AllocatedRegionTag;

    using TUnusedRegionHook = boost::intrusive::list_base_hook<boost::intrusive::tag<UnusedRegionTag>>;
    using TAllRegionsHook = boost::intrusive::list_base_hook<boost::intrusive::tag<RegionTag>>;
    using TFreeRegionHook = boost::intrusive::set_base_hook<boost::intrusive::tag<FreeRegionTag>>;
    using TAllocatedRegionHook = boost::intrusive::set_base_hook<boost::intrusive::tag<AllocatedRegionTag>>;

    struct RegionMetadata;
    struct RegionCompareBySize;
    struct RegionCompareByKey;

    using TUnusedRegionsList = boost::intrusive::list<RegionMetadata,
        boost::intrusive::base_hook<TUnusedRegionHook>,
        boost::intrusive::constant_time_size<true>>;
    using TRegionsList = boost::intrusive::list<RegionMetadata,
        boost::intrusive::base_hook<TAllRegionsHook>,
        boost::intrusive::constant_time_size<true>>;
    using TFreeRegionsMap = boost::intrusive::multiset<RegionMetadata,
        boost::intrusive::compare<RegionCompareBySize>,
        boost::intrusive::base_hook<TFreeRegionHook>,
        boost::intrusive::constant_time_size<true>>;
    using TAllocatedRegionsMap = boost::intrusive::set<RegionMetadata,
        boost::intrusive::compare<RegionCompareByKey>,
        boost::intrusive::base_hook<TAllocatedRegionHook>,
        boost::intrusive::constant_time_size<true>>;

    /**
     * Represented by an adjacency list (boost linked list).
     * Used to find region's free neighbours to coalesce on eviction.
     */
    TRegionsList all_regions;

    /**
     * Represented by a size multimap.
     * Used to find free region with at least requested size.
     */
    TFreeRegionsMap free_regions;

    /**
     * Represented by a keymap (boost multiset).
     */
    TAllocatedRegionsMap allocated_regions;

    /**
     * Represented by a LRU List (boost linked list).
     * Used to find the next region for eviction.
     * TODO Replace with exponentially-smoothed size-weighted LFU map.
     */
    TUnusedRegionsList unused_allocated_regions;

    struct MemoryChunk;
    std::list<MemoryChunk> chunks;

    struct InsertionAttempt;
    std::unordered_map<Key, std::shared_ptr<InsertionAttempt>, THash> insertion_attempts;

    mutable std::mutex mutex;

    pcg64 rng{randomSeed()};

    /**
     * Stats data
     */

    /// Max cache size.
    const size_t max_total_size;

    size_t total_chunks_size {0};
    size_t total_allocated_size {0};
    size_t total_size_in_use {0};

    std::atomic<size_t> total_size_currently_initialized {0};

    /// Value was in the cache.
    std::atomic<size_t> hits {0};

    /// Value we were waiting for was calculated by another thread.
    /// @note Also summed in \c hits.
    std::atomic<size_t> concurrent_hits {0};

    /// Value was not found in the cache
    std::atomic<size_t> misses {0};

    size_t allocations {0};
    size_t allocated_bytes {0};
    size_t evictions {0};
    size_t evicted_bytes {0};
    size_t secondary_evictions {0};

    /**
     * Represents an attempt to insert a new entry into cache.
     * Useful in 2 situation:
     * <ul>
     * <li>
     * Value for a given key was not found in the cache, but it may be produced by another thread.
     * We create this \c attempt and try to achieve a lock on its \c mutex.
     * On success we may find the \c attempt already has a \c value (= it was calculated by other thread).
     * </li>
     * <li>
     * Same as 1, but the \c attempt has no \c value. It simply means value is not present in cache, so
     * we approve this attempt and insert it.
     * </li>
     * </ul>
     *
     * @see \c GetOrSet
     */
    struct InsertionAttempt
    {
        explicit InsertionAttempt(CachingAllocator & cache_) noexcept : cache(cache_) {}

        CachingAllocator & cache;

        /// Mutex that protects \c is_disposed, \c value and \c refcount
        std::mutex mutex;

        bool is_disposed{false};
        MemoryRegionPtr value;
        size_t refcount {0};
    };

    /**
     * Responsible for removing a single \c InsertionAttempt from the \c insertion_attempts container.
     * The disposal may be performed via an explicit \c dispose call or in the destructor.
     *
     * Among several concurrent threads the first successful one is responsible for removal.
     * If they all fail, the last one is responsible.
     */
    struct InsertionAttemptDisposer
    {
        InsertionAttemptDisposer() noexcept = default;

        const Key * key {nullptr};

        std::shared_ptr<InsertionAttempt> attempt;

        bool attempt_disposed{false};

        /// Now this container is owning the \c attempt_ for a given \c key_.
        inline void acquire(const Key * key_, const std::shared_ptr<InsertionAttempt> & attempt_) noexcept
        {
            key = key_;
            attempt = attempt_;
            ++attempt->refcount;
        }

        inline void dispose() noexcept
        {
            attempt->cache.insertion_attempts.erase(*key);
            attempt->is_disposed = true;
            attempt_disposed = true;
        }

        ~InsertionAttemptDisposer() noexcept
        {
            if (!attempt)
                return;

            if (attempt_disposed)
                return;

            std::lock_guard attempt_lock(attempt->mutex);

            if (attempt->is_disposed)
                return;

            std::lock_guard cache_lock(attempt->cache.mutex);

            --attempt->refcount;

            if (attempt->refcount == 0)
                dispose();
        }
    };

    /**
     * Most low-level primitive of this allocator, represents a sort of \c mdspan of a certain memory part.
     * Can hold both free and occupied memory regions.
     * \c mmap s space of desired size in ctor and frees it in dtor.
     */
    struct MemoryChunk : private boost::noncopyable
    {
        void * ptr;
        size_t size;

        MemoryChunk(size_t size_, void * address_hint) : size(size_)
        {
            ptr = mmap(
                __builtin_assume_aligned(address_hint, ptrs_alignment),
                size,
                PROT_READ | PROT_WRITE, // NOLINT(hicpp-signed-bitwise)
                MAP_PRIVATE | // NOLINT(hicpp-signed-bitwise)
                MAP_ANONYMOUS |
                MAP_POPULATE, /*possible speedup on read-ahead*/
                /**fd */ -1,
                /**offset*/ 0);

            if (MAP_FAILED == ptr)
                DB::throwFromErrno(
                    "Allocator: Cannot mmap " +
                    formatReadableSizeWithBinarySuffix(size) + ".",
                    DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }

        ~MemoryChunk()
        {
            if (ptr && 0 != munmap(ptr, size))
                DB::throwFromErrno(
                    "Allocator: Cannot munmap " +
                    formatReadableSizeWithBinarySuffix(size) + ".",
                    DB::ErrorCodes::CANNOT_MUNMAP);
        }

        MemoryChunk(MemoryChunk && other) noexcept : ptr(other.ptr), size(other.size)
        {
            other.ptr = nullptr;
        }
    };

    /**
      * Represents a used (currently used) memory region of a \c MemoryChunk.
      * You can think of it as a view over some chunk's part (probably, of the whole chunk).
      * Modifies \c unused_allocated_regions in the cache according to region's \c refcount in ctor and dtor.
      */
    struct MemoryRegion : private boost::noncopyable
    {
        MemoryRegion(CachingAllocator & cache_, RegionMetadata & region_) noexcept
            : cache(cache_), metadata(region_)
        {
            ++metadata.refcount;

            if (metadata.refcount == 1 && metadata.TUnusedRegionHook::is_linked())
                cache.unused_allocated_regions.erase(cache.unused_allocated_regions.iterator_to(metadata));

            cache.total_size_in_use += metadata.size;
        }

        ~MemoryRegion() noexcept
        {
            std::lock_guard cache_lock(cache.mutex);

            --metadata.refcount;

            if (metadata.refcount == 0)
                cache.unused_allocated_regions.push_back(metadata);

            cache.total_size_in_use -= metadata.size;
        }

        [[nodiscard, gnu::assume_aligned(ptrs_alignment), gnu::pure]] void * ptr() { return metadata.ptr; }
        [[nodiscard, gnu::assume_aligned(ptrs_alignment), gnu::pure]] const void * ptr() const { return metadata.ptr; }

        [[nodiscard, gnu::pure]] size_t size() const { return metadata.size; }

        [[nodiscard, gnu::pure]] Key key() const { return metadata.key; }

        [[nodiscard, gnu::pure]] Mapped & mapped() { return metadata.mapped; }
        [[nodiscard, gnu::pure]] const Mapped & mapped() const { return metadata.mapped; }

    private:
        CachingAllocator & cache;
        RegionMetadata & metadata;
    };

    struct RegionCompareBySize
    {
        bool operator() (const RegionMetadata & a, const RegionMetadata & b) const noexcept { return a.size < b.size; }
        bool operator() (const RegionMetadata & a, size_t size) const noexcept { return a.size < size; }
        bool operator() (size_t size, const RegionMetadata & b) const noexcept { return size < b.size; }
    };

    struct RegionCompareByKey
    {
        bool operator() (const RegionMetadata & a, const RegionMetadata & b) const noexcept { return a.key < b.key; }
        bool operator() (const RegionMetadata & a, Key key) const noexcept { return a.key < key; }
        bool operator() (Key key, const RegionMetadata & b) const noexcept { return key < b.key; }
    };

    static constexpr auto region_metadata_disposer = [](RegionMetadata * ptr) { ptr->destroy(); };

    struct RegionMetadata : public TUnusedRegionHook, TAllRegionsHook, TFreeRegionHook, TAllocatedRegionHook // NOLINT(cppcoreguidelines-pro-type-member-init)
    {
        Key key;
        Mapped mapped;

        union
        {
            void * ptr {nullptr};
            char * char_ptr;
        };

        size_t size {0};
        size_t refcount {0};

        void * chunk {nullptr};

        [[nodiscard, gnu::pure]] bool operator< (const RegionMetadata & other) const noexcept { return size < other.size; }

        [[nodiscard, gnu::pure]] bool isFree() const noexcept { return TFreeRegionHook::is_linked(); }

        [[nodiscard, gnu::malloc]] static RegionMetadata * create() { return new RegionMetadata; }

        void destroy() { delete this; }

    private:
        RegionMetadata() = default;
        ~RegionMetadata() = default;
    };

    inline MemoryRegionPtr getImpl(const Key& key, InsertionAttemptDisposer& disposer, InsertionAttempt *& attempt)
    {
        {
            std::lock_guard cache_lock(mutex);

            auto region_it = allocated_regions.find(key, RegionCompareByKey());

            if (allocated_regions.end() != region_it)
            {
                ++hits;

                return std::make_shared<MemoryRegion>(*this, *region_it);
            }

            auto & insertion_attempt = insertion_attempts[key];

            if (!insertion_attempt)
                insertion_attempt = std::make_shared<InsertionAttempt>(*this);

            disposer.acquire(&key, insertion_attempt);
        }

        attempt = disposer.attempt.get();

        std::lock_guard attempt_lock(attempt->mutex);

        disposer.attempt_disposed = attempt->is_disposed;

        if (attempt->value)
        {
            /// Another thread already produced the value while we were acquiring the attempt's mutex.
            ++hits;
            ++concurrent_hits;

            return attempt->value;
        }

        ++misses;

        return {};
    }

    /**
     * Inserts \c region into \c free_regions container, probably coalescing it with adjacent free regions (one to the
     * right and one to the left).
     *
     * @param region Target region to insert. Must not be present in \c free_regions, \c allocated_regions and
     *      \c unused_allocated_regions.
     */
    inline void freeAndCoalesce(RegionMetadata & region) noexcept
    {
        auto target_region_it = all_regions.iterator_to(region);
        auto region_it = target_region_it;

        auto erase_free_region = [this, &region](auto region_iter) noexcept {
            if (region_iter->chunk != region.chunk || !region_iter->isFree())
                return;

            region.size += region_iter->size;
            region.char_ptr-= region_iter->size;

            free_regions.erase(free_regions.iterator_to(*region_iter));
            all_regions.erase_and_dispose(region_iter, region_metadata_disposer);
        };

        /// Coalesce with left neighbour
        if (all_regions.begin() != region_it)
            erase_free_region(--region_it);

        region_it = target_region_it;

        /// Coalesce with right neighbour
        if (all_regions.end() != region_it)
            erase_free_region(++region_it);

        free_regions.insert(region);
    }

    inline void evict(RegionMetadata & target) noexcept
    {
        total_allocated_size -= target.size;

        unused_allocated_regions.erase(unused_allocated_regions.iterator_to(target));

        if (target.TAllocatedRegionHook::is_linked())
            allocated_regions.erase(allocated_regions.iterator_to(target));

        ++evictions;
        evicted_bytes += target.size;

        freeAndCoalesce(target);
    }

    /**
     * Evicts region of at least \c requested_size from cache and returns it,
     * probably coalesced with nearby free regions.
     * While size is not enough, evicts adjacent regions at right, if any.
     * Target region is removed from \c unused_allocated_regions and \c allocated_regions and inserted into
     * \c free_regions.
     *
     * @see \c evict(RegionMetadata&)
     *
     * @return \c nullptr if there are no unused regions.
     * @return Target region otherwise.
     */
    inline RegionMetadata * evict(size_t requested) noexcept
    {
        if (unused_allocated_regions.empty())
            return nullptr;

        auto region_it = all_regions.iterator_to(unused_allocated_regions.front());

        while (true)
        {
            RegionMetadata & evicted = *region_it;
            evict(evicted);

            if (evicted.size >= requested)
                return &evicted;

            ++region_it;

            if (region_it == all_regions.end() ||
                region_it->chunk != evicted.chunk ||
                !region_it->TUnusedRegionHook::is_linked())
                return &evicted;

            ++secondary_evictions;
        }
    }

    [[nodiscard, gnu::pure]] inline void * ASLR()
    {
        return reinterpret_cast<void *>(
            std::uniform_int_distribution<size_t>(
            0x100000000000UL,
            0x700000000000UL)(rng));
    }

    /**
     * Allocates a \c MemoryChunk of specified size.
     * @return A free region, spanning through the whole allocated chunk.
     */
    RegionMetadata * addNewChunk(size_t size)
    {
        chunks.emplace_back(size, ASLR());
        MemoryChunk & chunk = chunks.back();

        total_chunks_size += size;

        RegionMetadata * free_region;

        try
        {
            free_region = RegionMetadata::create();
        }
        catch (...)
        {
            total_chunks_size -= size;
            chunks.pop_back();
            throw;
        }

        free_region->ptr = chunk.ptr;
        free_region->chunk = chunk.ptr;
        free_region->size = chunk.size;

        all_regions.push_back(*free_region);
        free_regions.insert(*free_region);

        return free_region;
    }

    RegionMetadata * allocateFromFreeRegion(RegionMetadata & free_region, size_t size)
    {
        if (free_region.size < size) return nullptr;

        ++allocations;
        allocated_bytes += size;

        if (free_region.size == size)
        {
            total_allocated_size += size;
            free_regions.erase(free_regions.iterator_to(free_region));
            return &free_region;
        }

        /// No try-catch needed
        RegionMetadata * allocated_region = RegionMetadata::create();

        total_allocated_size += size;

        allocated_region->ptr = free_region.ptr;
        allocated_region->chunk = free_region.chunk;
        allocated_region->size = size;

        free_regions.erase(free_regions.iterator_to(free_region));
        free_region.size -= size;
        free_region.char_ptr += size;
        free_regions.insert(free_region);

        all_regions.insert(all_regions.iterator_to(free_region), *allocated_region);

        return allocated_region;
    }

    [[nodiscard, gnu::const]] static constexpr size_t roundUp(size_t x, size_t rounding)
    {
        return (x + (rounding - 1)) / rounding * rounding;
    }

    /**
     * Main allocation routine.
     * @note Method does not insert allocated region to \c allocated_regions or \c unused_allocated_regions.
     * @return Desired region or \c nullptr.
     */
    RegionMetadata * allocate(size_t size)
    {
        size = roundUp(size, ptrs_alignment);

        auto free_region_it = free_regions.lower_bound(size, RegionCompareBySize());

        if (free_regions.end() != free_region_it)
            return allocateFromFreeRegion(*free_region_it, size);

        /// If nothing was found and total size of allocated chunks plus required size is lower than maximum,
        /// allocate a new chunk.
        size_t required_chunk_size = std::max(min_chunk_size, roundUp(size, page_size));

        if (total_chunks_size + required_chunk_size <= max_total_size)
        {
            /// Create free region spanning through chunk.
            RegionMetadata * free_region = addNewChunk(required_chunk_size);
            return allocateFromFreeRegion(*free_region, size);
        }

        /// Evict something from cache and continue.
        while (true)
        {
            RegionMetadata * res = evict(size);

            /// Nothing to evict. All cache is full and in use - cannot allocate memory.
            if (!res)
                return nullptr;

            /// Not enough. Evict more.
            if (res->size < size)
                continue;

            return allocateFromFreeRegion(*res, size);
        }
    }
};
