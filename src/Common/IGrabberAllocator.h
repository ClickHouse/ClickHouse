#pragma once

#include <atomic>
#include <cstddef>
#include <mutex>
#include <memory>
#include <random>
#include <type_traits>
#include <functional>
#include <unordered_map>

#include <sys/mman.h>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/noncopyable.hpp>
#include <ext/scope_guard.h>

#include <pcg_random.hpp>

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

namespace ga {

/// Default parameter for two of allocator's template parameters, serves as a placeholder.
struct runtime {};

static constexpr size_t const defaultMinChunkSize = 64 * 1024 * 1024;

template <class Value>
static constexpr size_t const defaultValueAlignment = std::max(16lu,  alignof(Value));

[[nodiscard, gnu::const]] void * defaultASLR(const pcg64& rng) noexcept
{
    return reinterpret_cast<void *>(
            std::uniform_int_distribution<size_t>(0x100000000000UL, 0x700000000000UL)(rng));
}

[[nodiscard, gnu::const]] static constexpr size_t roundUp(size_t x, size_t rounding) noexcept
{
    return (x + (rounding - 1)) / rounding * rounding;
}

struct Stats
{
    size_t total_chunks_size {0};
    size_t total_allocated_size {0};
    size_t total_currently_initialized_size {0};
    size_t total_currently_used_size {0};

    size_t chunks_count{0};
    size_t all_regions_count {0};
    size_t free_regions_count {0};
    size_t used_regions_count {0};
    size_t keyed_regions_count {0};

    size_t hits {0};
    size_t concurrent_hits {0};
    size_t misses {0};

    size_t allocations {0};
    size_t allocated_bytes {0};
    size_t evictions {0};
    size_t evicted_bytes {0};
    size_t secondary_evictions {0};
};
}

/**
 * Constraints on allocator's template parameters
 */

template <class T> concept GAKey = GADefault; //TODO Form constraints

template <class T> concept GAValue = requires(T x) { };

/// @anchor ga_key_hash
template <class Hash, class Value>
concept GAKeyHash = requires(const Value& v) {
    { Hash{}.operator()(v) } -> std::convertible_to<std::size_t>;
};

/// @anchor ga_size_func
template <class F, class Value>
concept GASizeFunction = std::is_same<F, ga::runtime> || requires(const Value& v) {
    {F(v)} -> std::convertible_to<std::size_t>;
};

/// @anchor ga_init_func
template <class F, class Value>
concept GAInitFunction = std::is_same<F, ga::runtime> || requires(void * address_hint) {
    F(address_hint, Value*);
};

/// @anchor ga_aslr_func
template <class T> concept GAAslrFunction = requires(const pcg64& rng) {
    {T()} -> std::is_same<void *>;
};

/**
 * @brief This class represents a tool combining a reference-counted memory block cache integrated with a mmap-backed
 *        memory allocator. Overall memory limit, begin set by the user, is constant, so this tool also performs
 *        cache entries eviction if needed.
 *
 * @section motivation Motivation
 * @subsection general General
 *
 *   - Domain-specific cache has a memory usage limit (and we want it to include allocator fragmentation overhead).
 *   - Memory fragmentation can be bridged over using cache eviction.
 *   - We do not waste space for reclaimed memory regions. Ordinarily, they'd get into default allocator raw cache
 *     after being freed.
 *   - Direct memory allocation serves for debugging purposes: placing mmap'd memory far from malloc'd facilitates
 *     revealing memory stomping bugs.
 *
 * @subsection size_and_init #SizeFunction and #InitFunction
 *
 * If one is specified, interface is thought to have a @e compile-time function (e.g. @c sizeof(#Value) or so for
 * #SizeFunction, etc).
 * As opposed to that, the default value is ga::runtime which indicates that such a function needs to access the
 * object's state, so it should be passed as a parameter to the routines.
 *
 *
 *
 * @tparam TKey Object that will be used to @e quickly address #Value.
 * @tparam TValue Type of objects being stored (usually thought to be some child of PODArray).
 *
 * @tparam KeyHash Struct generating a compile-time size_t hash for #Key. By default, std::hash is used.
 *         See @ref ga_key_hash for signature requirements.
 *
 * @tparam SizeFunction Struct generating a size_t that approximates (as an upper bound) #Value's size.
 *         See @ref ga_size_func for signature requirements.
 *
 * @tparam ASLRFunction Function that, given a random number generator, outputs the address in heap, which
 *         will be used as a starting point to place the object.
 *         Must meet the requirements of gnu::pure function attribute.
 *         See @ref ga_aslt_func for signature requirements.
 *
 * @note Cache is not NUMA friendly.
 */
template <
    GAKey TKey,
    GAValue TValue,

    GAKeyHash KeyHash = std::hash<Key>,

    GASizeFunction SizeFunction = ga::runtime,
    GAInitFunction InitFunction = ga::runtime,
    GAAslrFunction ASLRFunction = ga::defaultASLR,

    size_t MinChunkSize = ga::defaultMinChunkSize,
    size_t ValueAlignment = ga::defaultValueAlignment<Value>>
class IGrabberAllocator : private boost::noncopyable
{
private:
    static constexpr const size_t page_size = 4096;

    class RegionMetadata;
    static constexpr auto region_metadata_disposer = [](RegionMetadata * ptr) { ptr->destroy(); };

    mutable std::mutex mutex;

    pcg64 rng{randomSeed()};

/**
 * Constructing, destructing, and usings.
 */
public:
    using Key = TKey;
    using Value = TValue;

    /**
     * @brief Output type, tracks reference counf (@ref overview) via a custom Deleter.
     */
    using ValuePtr = std:shared_ptr<Value>;

    constexpr CachingAllocator(size_t max_cache_size_) noexcept: max_cache_size(max_cache_size_) {}

    constexpr ~CachingAllocator() noexcept
    {
        std::lock_guard _(mutex);

        used_regions.clear();
        unused_allocated_regions.clear();
        free_regions.clear();

        all_regions.clear_and_dispose(region_metadata_disposer);
    }

/**
 * Statistics storage.
 */
private:
    const size_t max_cache_size;

    size_t total_chunks_size {0};
    size_t total_allocated_size {0};
    size_t total_size_in_use {0};

    std::atomic_size_t total_size_currently_initialized {0};

    /// Value was in the cache.
    std::atomic_size_t hits {0};

    /// Value we were waiting for was calculated by another thread.
    /// @note Also summed in #hits.
    std::atomic_size_t concurrent_hits {0};

    /// Value was not found in the cache.
    std::atomic_size_t misses {0};

    size_t allocations {0};
    size_t allocated_bytes {0};
    size_t evictions {0};
    size_t evicted_bytes {0};
    size_t secondary_evictions {0};

/**
 * Reset and statistics utilities
 */
public:
    /// @return Total size (in bytes) occupied by the cache (does NOT include metadata size).
    constexpr size_t getSizeInUse() const noexcept
    {
        return total_size_in_use;
    }

    /// @return Count of pairs (#Key, #Value) that are currently in use.
    constexpr size_t getUsedRegionsCount() const noexcept
    {
        return used_regions.size();
    }

    /// Clears the cache.
    constexpr void reset() noexcept
    {
        std::lock_guard lock(mutex);

        insertion_attempts.clear();
        chunks.clear();

        free_regions.clear();
        used_regions.clear();
        unused_allocated_regions.clear();

        all_regions.clear_and_dispose(region_metadata_disposer);

        total_chunks_size = 0;
        total_allocated_size = 0;
        total_size_in_use = 0;

        total_size_currently_initialized.store(0, std::memory_order_relaxed);

        hits.store(0, std::memory_order_relaxed);
        concurrent_hits.store(0, std::memory_order_relaxed);
        misses.store(0, std::memory_order_relaxed);

        allocations  = 0;
        allocated_bytes = 0;
        evictions = 0;
        evicted_bytes = 0;
        secondary_evictions = 0;
    }

    /**
     * @note Metadata is allocated by a usual allocator (malloc/new) so its memory usage is not accounted.
     */
    [[nodiscard]] constexpr ga::Stats getStats() const noexcept
    {
        std::lock_guard cache_lock(mutex);

        ga::Stats out = {};

        out.total_chunks_size = total_chunks_size;
        out.total_allocated_size = total_allocated_size;
        out.total_size_currently_initialized = total_size_currently_initialized.load(std::memory_order_relaxed);
        out.total_size_in_use = total_size_in_use;

        out.num_chunks = chunks.size();
        out.num_regions = all_regions.size();
        out.num_free_regions = free_regions.size();
        out.num_regions_in_use = all_regions.size() - free_regions.size() - unused_allocated_regions.size();
        out.num_keyed_regions = used_regions.size();

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

/**
 * Memory storage.
 */
private:
    /**
     * Each memory region can be:
     * - Free: not holding any data.
     * - Allocated, but unused (can be evicted).
     * - Allocated and used (addressed by #Key, can't be evicted).
     */

    struct UnusedRegionTag;
    struct RegionTag;
    struct FreeRegionTag;
    struct AllocatedRegionTag;

    using TUnusedRegionHook    = boost::intrusive::list_base_hook<boost::intrusive::tag<UnusedRegionTag>>;
    using TAllRegionsHook      = boost::intrusive::list_base_hook<boost::intrusive::tag<RegionTag>>;
    using TFreeRegionHook      = boost::intrusive::set_base_hook< boost::intrusive::tag<FreeRegionTag>>;
    using TAllocatedRegionHook = boost::intrusive::set_base_hook< boost::intrusive::tag<AllocatedRegionTag>>;

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
     * @brief Represented by an adjacency list (boost linked list). Used to find region's free neighbours to coalesce
     *        on eviction.
     *
     * @anchor ds_start.
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
    TAllocatedRegionsMap used_regions;

    /**
     * Represented by a LRU List (boost linked list).
     * Used to find the next region for eviction.
     * TODO Replace with exponentially-smoothed size-weighted LFU map.
     */
    TUnusedRegionsList unused_allocated_regions;

    /**
     * Used to identify metadata associated with some #Value (needed while deleting #ValuePtr).
     * @see IGrabberAllocator::getImpl
     */
    std::unordered_map<Value*, RegionMetadata*> value_to_region;

    struct MemoryChunk;
    std::list<MemoryChunk> chunks;

    struct InsertionAttempt;
    std::unordered_map<Key, std::shared_ptr<InsertionAttempt>, KeyHash> insertion_attempts;

/**
 * Getting and setting utilities
 */
public:
    inline ValuePtr get(const Key& key)
    {
        std::lock_guard lock(mutex);

        InsertionAttemptDisposer disposer;
        InsertionAttempt * attempt;

        return getImpl(key, disposer, attempt);
    }

    using GetOrSetRet = std::pair<ValuePtr, bool>;

    template <std::enable_if_t<!std::is_same_v<SizeFunction, ga::runtime> &&
                               !std::is_same_v<InitFunction, ga::runtime>>>
    inline GetOrSetRet getOrSet(const Key & key)
    {
        return getOrSet(key, SizeFunction, InitFunction);
    }

    template <std::enable_if_t<!std::is_same_v<SizeFunction, ga::runtime> &&
                                std::is_same_v<InitFunction, ga::runtime>>>
    inline GetOrSetRet getOrSet(const Key & key, GAInitFunction auto && init_func)
    {
        return getOrSet(key, SizeFunction, init_func);
    }

    template <std::enable_if_t<!std::is_same_v<SizeFunction, ga::runtime> &&
                                std::is_same_v<InitFunction, ga::runtime>>>
    inline GetOrSetRet getOrSet(const Key & key, GAInitFunction auto && init_func)
    {
        return getOrSet(key, SizeFunction, init_func);
    }

    /**
     * @brief If the value for the key is in the cache, returns it.
     *        Otherwise, calls #get_size to obtain required size of storage for the key, then allocates storage and
     *        calls #initialize for necessary initialization before data from cache could be used.
     *
     * @note Only one of several concurrent threads calling this method will call #get_size or #initialize functors.
     *       Others will wait for that call to complete and will use its result (this helps to prevent cache stampede).
     *
     * @note Exceptions that occur in callback functors (#get_size and #initialize) will be propagated to the caller.
     *       Another thread from the set of concurrent threads will then try to call its callbacks et cetera.
     *
     * @note During insertion, each key is locked - to avoid parallel initialization of regions for same key.
     *
     * @param key Key that is used to find the target value.
     *
     * @param get_size Callback functor that is called if value with a given #key was neither found in cache
     *      nor produced by any of other threads.
     *
     * @param initialize Callback function that is called if
     *      <ul>
     *      <li>#get_size is called and doesn't throw.</li>
     *      <li>Allocator did not fail to allocate another memory region.</li>
     *      </ul>
     *
     * @return Cached value.
     * @return Bool indicating whether the value was produced during the call. <br>
     *      False on cache hit (including a concurrent cache hit), true on cache miss).
     */
    inline GetOrSetRet getOrSet(const Key & key, SizeFunction auto && get_size, InitFunction auto && initialize)
    {
        InsertionAttemptDisposer disposer;
        InsertionAttempt * attempt;

        if (ValuePtr out = getImpl(key, disposer, attempt); out)
            return {out, false}; // value was found in the cache.

        /// No try-catch here because it is not needed.
        size_t size = get_size();

        RegionMetadata * region = nullptr;

        {
            std::lock_guard cache_lock(mutex);
            region = allocate(size);
        }

        /// Cannot allocate memory.
        if (!region) return {};

        region->key = key;

        {
            total_size_currently_initialized.fetch_add(size, std::memory_order_release);
            SCOPE_EXIT({ total_size_currently_initialized.fetch_sub(size, std::memory_order_release); });

            try
            {
                initialize(region->ptr, region->value);
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
            onSharedValueCreate(region);
            attempt->value = std::make_shared<Value>(&region->value, onValueDelete);

            /// Insert the new value only if the attempt is still present in #insertion_attempts.
            /// (may be absent because of a concurrent reset() call).
            auto attempt_it = insertion_attempts.find(key);

            if (insertion_attempts.end() != attempt_it && attempt_it->second.get() == attempt)
                used_regions.insert(*region);

            if (!attempt->is_disposed)
                disposer.dispose();

            return {attempt->value, true};
        }
        catch (...)
        {
            if (region->TAllocatedRegionHook::is_linked())
                used_regions.erase(used_regions.iterator_to(*region));

            freeAndCoalesce(*region);

            throw;
        }
    }

/**
 * Get implementation, value deleter for shared_ptr.
 */
private:
    constexpr void onSharedValueCreate(RegionMetadata& metadata) noexcept
    {
        std::lock_guard cache_lock(cache.mutex);

        ++metadata.refcount;

        if (metadata.refcount == 1 && metadata.TUnusedRegionHook::is_linked())
        {
            value_to_region.emplace_back(&metadata.value, &metadata);
            unused_allocated_regions.erase(unused_allocated_regions.iterator_to(metadata));
        }

        total_size_in_use += metadata.size;
    }

    constexpr void onValueDelete(Value * value) noexcept
    {
        std::lock_guard cache_lock(cache.mutex);

        auto it = value_to_region.find(value);

        // it != value_to_region.end() because there exists at least one shared_ptr using this value (the one
        // invoking this function), thus value_to_region contains a metadata struct associated with #value.

        RegionMetadata& metadata = **it;

        --metadata.refcount;

        if (metadata.refcount == 0)
        {
            value_to_region.erase(it);
            cache.unused_allocated_regions.push_back(metadata);
        }

        cache.total_size_in_use -= metadata.size;

        delete value;
    }

    inline ValuePtr getImpl(const Key& key, InsertionAttemptDisposer& disposer, InsertionAttempt *& attempt)
    {
        {
            std::lock_guard cache_lock(mutex);

            if (auto it = used_regions.find(key, RegionCompareByKey()); used_regions.end() != it)
            {
                /// Value found in cache

                hits.fetch_add(1, std::memory_order_relaxed);

                RegionMetadata& metadata = *it;

                onSharedValueCreate(metadata);

                return std::make_shared<Value>(&metadata->value, onValueDelete);
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
            hits.fetch_add(1, std::memory_order_relaxed);
            concurrent_hits.fetch_add(1, std::memory_order_relaxed);

            return attempt->value;
        }

        misses.fetch_add(1, std::memory_order_relaxed);

        return nullptr;
    }

/**
 * Tokens and attempts
 */
private:
    /**
     * @brief Represents an attempt to insert a new entry into the cache.
     *
     * Useful in 2 situation:
     *
     *  - Value for a given key was not found in the cache, but it may be produced by another thread.
     *    We create this \c attempt and try to achieve a lock on its #mutex.
     *    On success we may find the \c attempt already having a #value (= it was calculated by other thread).
     *  - Same as 1, but the \c attempt has no # value. It simply means value is not present in cache, so we approve
     *    this attempt and insert it.
     */
    struct InsertionAttempt
    {
        constexpr InsertionAttempt(IGrabberAllocator & alloc_) noexcept : alloc(alloc_) {}

        IGrabberAllocator & alloc;

        /// Protects #is_disposed, #value and #refcount.
        std::mutex mutex;

        /// @c true if this attempt is holding a value.
        bool is_disposed{false};

        /// How many InsertionAttemptDisposer's want to remove this attempt.
        size_t refcount {0};

        ValuePtr value;
    };

    /**
     * @brief Responsible for removing a single InsertionAttempt from the #insertion_attempts container.
     *        The disposal may be performed via an explicit #dispose call or in the destructor.
     *
     * @note Among several concurrent threads the first successful one is responsible for removal.
     *       If they all fail, the last one is responsible.
     */
    struct InsertionAttemptDisposer
    {
        constexpr InsertionAttemptDisposer() noexcept = default;

        const Key * const key {nullptr};
        bool attempt_disposed{false};

        std::shared_ptr<InsertionAttempt> attempt;

        /// Now this container is owning the #attempt_ for a given #key_.
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
 * Memory-related structures.
 */
private:
    /**
     * @brief Most low-level primitive of this allocator, represents a sort of std::span of a certain memory part.
     *        Can hold both free and occupied memory regions. mmaps space of desired size in ctor and frees it in dtor.
     */
    struct MemoryChunk : private boost::noncopyable
    {
        void * ptr;
        size_t size;

        constexpr MemoryChunk(size_t size_, void * address_hint) : size(size_)
        {
            ptr = mmap(
                __builtin_assume_aligned(address_hint, ptrs_alignment),
                size,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE |
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

        constexpr MemoryChunk(MemoryChunk && other) noexcept : ptr(other.ptr), size(other.size)
        {
            other.ptr = nullptr;
        }
    };

    /**
     * @brief Element referenced in all intrusive containers here.
     */
    struct RegionMetadata :
        public TUnusedRegionHook,
        public TAllRegionsHook,
        public TFreeRegionHook,
        public TAllocatedRegionHook
    {
        Key key;
        Value value;

        union {
            void * ptr {nullptr};

            /// Used in IGrabberAllocator::freeAndCoalesce.
            char * char_ptr;
        };

        size_t size {0};

        /// How many outer users reference this object's #value?
        size_t refcount {0};

        void * chunk {nullptr};

        [[nodiscard, gnu::pure]] constexpr bool operator< (const RegionMetadata & other) const noexcept { return size < other.size; }

        [[nodiscard, gnu::pure]] constexpr bool isFree() const noexcept { return TFreeRegionHook::is_linked(); }

        [[nodiscard, gnu::malloc]] static RegionMetadata * create() { return new RegionMetadata; }

        void destroy() { delete this; }

    private:
        RegionMetadata() = default;
        ~RegionMetadata() = default;
    };

    struct RegionCompareBySize
    {
        constexpr bool operator() (const RegionMetadata & a, const RegionMetadata & b) const noexcept { return a.size < b.size; }
        constexpr bool operator() (const RegionMetadata & a, size_t size) const noexcept { return a.size < size; }
        constexpr bool operator() (size_t size, const RegionMetadata & b) const noexcept { return size < b.size; }
    };

    struct RegionCompareByKey
    {
        constexpr bool operator() (const RegionMetadata & a, const RegionMetadata & b) const noexcept { return a.key < b.key; }
        constexpr bool operator() (const RegionMetadata & a, Key key) const noexcept { return a.key < key; }
        constexpr bool operator() (Key key, const RegionMetadata & b) const noexcept { return key < b.key; }
    };

/**
 * Memory modifying operations.
 */
private:
    /**
     * @brief Main allocation routine. Allocates from free region (possibly creating a new one).
     * @note Method does not insert allocated region to #used_regions or #unused_allocated_regions.
     * @return Desired region or std::nullptr.
     */
    constexpr RegionMetadata * allocate(size_t size)
    {
        size = ga::roundUp(size, ValueAlignment);

        if (auto it = free_regions.lower_bound(size, RegionCompareBySize()); free_regions.end() != it)
            return allocateFromFreeRegion(*it, size);

        /// If nothing was found and total size of allocated chunks plus required size is lower than maximum,
        /// allocate from a newly created region spanning through the chunk.
        if (size_t req = std::max(min_chunk_size, ga::roundUp(size, page_size)); total_chunks_size + req <= max_total_size)
            return allocateFromFreeRegion(*addNewChunk(req), size);

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

    /**
     * @brief Given a free #free_region, occupies its part of size #size.
     */
    constexpr RegionMetadata * allocateFromFreeRegion(RegionMetadata & free_region, size_t size)
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

    /**
     * @brief Allocates a MemoryChunk of specified size.
     * @return A free region, spanning through the whole allocated chunk.
     * @post #all_regions and #free_regions contain target region.
     */
    constexpr RegionMetadata * addNewChunk(size_t size)
    {
        chunks.emplace_back(size, ASLRFunction());
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

    /**
     * @brief Evicts region of at least #requested size from cache and returns it, probably coalesced with nearby free
     *        regions. While size is not enough, evicts adjacent regions at right, if any.
     *
     * @post Target region is removed from #unused_allocated_regions and #used_regions and inserted into #free_regions.
     *
     * @return std::nullptr if there are no unused regions.
     * @return Target region otherwise.
     */
    constexpr RegionMetadata * evict(size_t requested) noexcept
    {
        if (unused_allocated_regions.empty())
            return nullptr;

        auto region_it = all_regions.iterator_to(unused_allocated_regions.front());

        while (true)
        {
            RegionMetadata & evicted = *region_it;

            total_allocated_size -= evicted.size;

            unused_allocated_regions.erase(unused_allocated_regions.iterator_to(evicted));

            if (target.TAllocatedRegionHook::is_linked())
                used_regions.erase(used_regions.iterator_to(evicted));

            ++evictions;
            evicted_bytes += evicted.size;

            freeAndCoalesce(evicted);

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

    /**
     * @brief Inserts #region into #free_regions container, probably coalescing it with adjacent free regions (one to
     *        the right and one to the left).
     *
     * @pre #region must not be present in #free_regions, #used_regions and #unused_allocated_regions.
     *
     * @param region Target region to insert.
     */
    constexpr void freeAndCoalesce(RegionMetadata & region) noexcept
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
        if (all_regions.end() != ++region_it)
            erase_free_region(region_it);

        free_regions.insert(region);
    }
};

