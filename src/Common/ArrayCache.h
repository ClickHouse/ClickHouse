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
#include <base/scope_guard.h>

#include <base/getPageSize.h>

#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Common/formatReadable.h>

/// Required for older Darwin builds, that lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_ALLOCATE_MEMORY;
        extern const int CANNOT_MUNMAP;
    }
}


/** Cache for variable length memory regions (contiguous arrays of bytes).
  * Example: cache for data read from disk, cache for decompressed data, etc.
  * It combines cache and allocator: allocates memory by itself without use of malloc/new.
  *
  * Motivation:
  * - cache has specified memory usage limit and we want this limit to include allocator fragmentation overhead;
  * - the cache overcomes memory fragmentation by cache eviction;
  * - there is no sense for reclaimed memory regions to be cached internally by usual allocator (malloc/new);
  * - by allocating memory directly with mmap, we could place it in virtual address space far
  *   from other (malloc'd) memory - this helps debugging memory stomping bugs
  *   (your program will likely hit unmapped memory and get segfault rather than silent cache corruption)
  *
  * Implementation:
  *
  * Cache is represented by list of mmapped chunks.
  * Each chunk holds free and occupied memory regions. Contiguous free regions are always coalesced.
  *
  * Each region could be linked by following metadata structures:
  * 1. LRU list - to find next region for eviction. NOTE Replace with exponentially-smoothed size-weighted LFU map.
  * 2. Adjacency list - to find neighbour free regions to coalesce on eviction.
  * 3. Size multimap - to find free region with at least requested size.
  * 4. Key map - to find element by key.
  *
  * Each region has:
  * - size;
  * - refcount: region could be evicted only if it is not used anywhere;
  * - chunk address: to check if regions are from same chunk.
  *
  * During insertion, each key is locked - to avoid parallel initialization of regions for same key.
  *
  * On insertion, we search for free region of at least requested size.
  * If nothing was found, we evict oldest unused region; if not enough size, we evict it neighbours; and try again.
  *
  * Metadata is allocated by usual allocator and its memory usage is not accounted.
  *
  * Caveats:
  * - cache is not NUMA friendly.
  *
  * Performance: few million ops/sec from single thread, less in case of concurrency.
  * Fragmentation is usually less than 10%.
  */

template <typename Key, typename Payload>
class ArrayCache : private boost::noncopyable
{
private:
    struct LRUListTag;
    struct AdjacencyListTag;
    struct SizeMultimapTag;
    struct KeyMapTag;

    using LRUListHook = boost::intrusive::list_base_hook<boost::intrusive::tag<LRUListTag>>;
    using AdjacencyListHook = boost::intrusive::list_base_hook<boost::intrusive::tag<AdjacencyListTag>>;
    using SizeMultimapHook = boost::intrusive::set_base_hook<boost::intrusive::tag<SizeMultimapTag>>;
    using KeyMapHook = boost::intrusive::set_base_hook<boost::intrusive::tag<KeyMapTag>>;

    struct RegionMetadata : public LRUListHook, AdjacencyListHook, SizeMultimapHook, KeyMapHook
    {
        Key key;
        Payload payload;

        union
        {
            void * ptr;
            char * char_ptr;
        };
        size_t size;
        size_t refcount = 0;
        void * chunk;

        bool operator< (const RegionMetadata & other) const { return size < other.size; }

        bool isFree() const { return SizeMultimapHook::is_linked(); }

        static RegionMetadata * create()
        {
            return new RegionMetadata;
        }

        void destroy()
        {
            delete this;
        }

    private:
         RegionMetadata() = default;
         ~RegionMetadata() = default;
    };

    struct RegionCompareBySize
    {
        bool operator() (const RegionMetadata & a, const RegionMetadata & b) const { return a.size < b.size; }
        bool operator() (const RegionMetadata & a, size_t size) const { return a.size < size; }
        bool operator() (size_t size, const RegionMetadata & b) const { return size < b.size; }
    };

    struct RegionCompareByKey
    {
        bool operator() (const RegionMetadata & a, const RegionMetadata & b) const { return a.key < b.key; }
        bool operator() (const RegionMetadata & a, Key key) const { return a.key < key; }
        bool operator() (Key key, const RegionMetadata & b) const { return key < b.key; }
    };

    using LRUList = boost::intrusive::list<RegionMetadata,
        boost::intrusive::base_hook<LRUListHook>, boost::intrusive::constant_time_size<true>>;
    using AdjacencyList = boost::intrusive::list<RegionMetadata,
        boost::intrusive::base_hook<AdjacencyListHook>, boost::intrusive::constant_time_size<true>>;
    using SizeMultimap = boost::intrusive::multiset<RegionMetadata,
        boost::intrusive::compare<RegionCompareBySize>, boost::intrusive::base_hook<SizeMultimapHook>, boost::intrusive::constant_time_size<true>>;
    using KeyMap = boost::intrusive::set<RegionMetadata,
        boost::intrusive::compare<RegionCompareByKey>, boost::intrusive::base_hook<KeyMapHook>, boost::intrusive::constant_time_size<true>>;

    /** Each region could be:
      * - free: not holding any data;
      * - allocated: having data, addressed by key;
      * -- allocated, in use: holded externally, could not be evicted;
      * -- allocated, not in use: not holded, could be evicted.
      */

    /** Invariants:
      * adjacency_list contains all regions
      * size_multimap contains free regions
      * key_map contains allocated regions
      * lru_list contains allocated regions, that are not in use
      */

    LRUList lru_list;
    AdjacencyList adjacency_list;
    SizeMultimap size_multimap;
    KeyMap key_map;

    mutable std::mutex mutex;

    pcg64 rng{randomSeed()};

    struct Chunk : private boost::noncopyable
    {
        void * ptr;
        size_t size;

        Chunk(size_t size_, void * address_hint) : size(size_)
        {
            ptr = mmap(address_hint, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (MAP_FAILED == ptr)
                DB::throwFromErrno(fmt::format("Allocator: Cannot mmap {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }

        ~Chunk()
        {
            if (ptr && 0 != munmap(ptr, size))
                DB::throwFromErrno(fmt::format("Allocator: Cannot munmap {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_MUNMAP);
        }

        Chunk(Chunk && other) noexcept : ptr(other.ptr), size(other.size)
        {
            other.ptr = nullptr;
        }
    };

    using Chunks = std::list<Chunk>;
    Chunks chunks;

    size_t total_chunks_size = 0;
    size_t total_allocated_size = 0;
    std::atomic<size_t> total_size_currently_initialized {0};
    size_t total_size_in_use = 0;

    /// Max size of cache.
    const size_t max_total_size;

    /// We will allocate memory in chunks of at least that size.
    /// 64 MB makes mmap overhead comparable to memory throughput.
    static constexpr size_t min_chunk_size = 64 * 1024 * 1024;

    /// Cache stats.
    std::atomic<size_t> hits {0};            /// Value was in cache.
    std::atomic<size_t> concurrent_hits {0}; /// Value was calculated by another thread and we was waiting for it. Also summed in hits.
    std::atomic<size_t> misses {0};

    /// For whole lifetime.
    size_t allocations = 0;
    size_t allocated_bytes = 0;
    size_t evictions = 0;
    size_t evicted_bytes = 0;
    size_t secondary_evictions = 0;


public:
    /// Holds region as in use. Regions in use could not be evicted from cache.
    /// In constructor, increases refcount and if it becomes non-zero, remove region from lru_list.
    /// In destructor, decreases refcount and if it becomes zero, insert region to lru_list.
    struct Holder : private boost::noncopyable
    {
        Holder(ArrayCache & cache_, RegionMetadata & region_) : cache(cache_), region(region_)
        {
            if (++region.refcount == 1 && region.LRUListHook::is_linked())
                cache.lru_list.erase(cache.lru_list.iterator_to(region));
            cache.total_size_in_use += region.size;
        }

        ~Holder()
        {
            std::lock_guard cache_lock(cache.mutex);
            if (--region.refcount == 0)
                cache.lru_list.push_back(region);
            cache.total_size_in_use -= region.size;
        }

        void * ptr() { return region.ptr; }
        const void * ptr() const { return region.ptr; }
        size_t size() const { return region.size; }
        Key key() const { return region.key; }
        Payload & payload() { return region.payload; }
        const Payload & payload() const { return region.payload; }

    private:
        ArrayCache & cache;
        RegionMetadata & region;
    };

    using HolderPtr = std::shared_ptr<Holder>;
private:

    /// Represents pending insertion attempt.
    struct InsertToken
    {
        explicit InsertToken(ArrayCache & cache_) : cache(cache_) {}

        std::mutex mutex;
        bool cleaned_up = false; /// Protected by the token mutex
        HolderPtr value; /// Protected by the token mutex

        ArrayCache & cache;
        size_t refcount = 0; /// Protected by the cache mutex
    };

    using InsertTokens = std::unordered_map<Key, std::shared_ptr<InsertToken>>;
    InsertTokens insert_tokens;

    /// This class is responsible for removing used insert tokens from the insert_tokens map.
    /// Among several concurrent threads the first successful one is responsible for removal. But if they all
    /// fail, then the last one is responsible.
    struct InsertTokenHolder
    {
        const Key * key = nullptr;
        std::shared_ptr<InsertToken> token;
        bool cleaned_up = false;

        InsertTokenHolder() = default;

        void acquire(const Key * key_, const std::shared_ptr<InsertToken> & token_, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
        {
            key = key_;
            token = token_;
            ++token->refcount;
        }

        void cleanup([[maybe_unused]] std::lock_guard<std::mutex> & token_lock, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
        {
            token->cache.insert_tokens.erase(*key);
            token->cleaned_up = true;
            cleaned_up = true;
        }

        ~InsertTokenHolder()
        {
            if (!token)
                return;

            if (cleaned_up)
                return;

            std::lock_guard token_lock(token->mutex);

            if (token->cleaned_up)
                return;

            std::lock_guard cache_lock(token->cache.mutex);

            --token->refcount;
            if (token->refcount == 0)
                cleanup(token_lock, cache_lock);
        }
    };

    friend struct InsertTokenHolder;


    static size_t roundUp(size_t x, size_t rounding)
    {
        return (x + (rounding - 1)) / rounding * rounding;
    }

    /// Sizes and addresses of allocated memory will be aligned to specified boundary.
    static constexpr size_t alignment = 16;


    /// Precondition: region is not in lru_list, not in key_map, not in size_multimap.
    /// Postcondition: region is not in lru_list, not in key_map,
    ///  inserted into size_multimap, possibly coalesced with adjacent free regions.
    void freeRegion(RegionMetadata & region) noexcept
    {
        auto adjacency_list_it = adjacency_list.iterator_to(region);

        auto left_it = adjacency_list_it;
        auto right_it = adjacency_list_it;

        //size_t was_size = region.size;

        if (left_it != adjacency_list.begin())
        {
            --left_it;

            //std::cerr << "left_it->isFree(): " << left_it->isFree() << "\n";

            if (left_it->chunk == region.chunk && left_it->isFree())
            {
                region.size += left_it->size;
                region.char_ptr-= left_it->size;
                size_multimap.erase(size_multimap.iterator_to(*left_it));
                adjacency_list.erase_and_dispose(left_it, [](RegionMetadata * elem) { elem->destroy(); });
            }
        }

        ++right_it;
        if (right_it != adjacency_list.end())
        {
            //std::cerr << "right_it->isFree(): " << right_it->isFree() << "\n";

            if (right_it->chunk == region.chunk && right_it->isFree())
            {
                region.size += right_it->size;
                size_multimap.erase(size_multimap.iterator_to(*right_it));
                adjacency_list.erase_and_dispose(right_it, [](RegionMetadata * elem) { elem->destroy(); });
            }
        }

        //std::cerr << "size is enlarged: " << was_size << " -> " << region.size << "\n";

        size_multimap.insert(region);
    }


    void evictRegion(RegionMetadata & evicted_region) noexcept
    {
        total_allocated_size -= evicted_region.size;

        lru_list.erase(lru_list.iterator_to(evicted_region));

        if (evicted_region.KeyMapHook::is_linked())
            key_map.erase(key_map.iterator_to(evicted_region));

        ++evictions;
        evicted_bytes += evicted_region.size;

        freeRegion(evicted_region);
    }


    /// Evict region from cache and return it, coalesced with nearby free regions.
    /// While size is not enough, evict adjacent regions at right, if any.
    /// If nothing to evict, returns nullptr.
    /// Region is removed from lru_list and key_map and inserted into size_multimap.
    RegionMetadata * evictSome(size_t requested_size) noexcept
    {
        if (lru_list.empty())
            return nullptr;

        /*for (const auto & elem : adjacency_list)
            std::cerr << (!elem.SizeMultimapHook::is_linked() ? "\033[1m" : "") << elem.size << (!elem.SizeMultimapHook::is_linked() ? "\033[0m " : " ");
        std::cerr << '\n';*/

        auto it = adjacency_list.iterator_to(lru_list.front());

        while (true)
        {
            RegionMetadata & evicted_region = *it;
            evictRegion(evicted_region);

            if (evicted_region.size >= requested_size)
                return &evicted_region;

            ++it;
            if (it == adjacency_list.end() || it->chunk != evicted_region.chunk || !it->LRUListHook::is_linked())
                return &evicted_region;

            ++secondary_evictions;
        }
    }


    /// Allocates a chunk of specified size. Creates free region, spanning through whole chunk and returns it.
    RegionMetadata * addNewChunk(size_t size)
    {
        /// ASLR by hand.
        void * address_hint = reinterpret_cast<void *>(std::uniform_int_distribution<size_t>(0x100000000000UL, 0x700000000000UL)(rng));

        chunks.emplace_back(size, address_hint);
        Chunk & chunk = chunks.back();

        total_chunks_size += size;

        /// Create free region spanning through chunk.
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

        adjacency_list.push_back(*free_region);
        size_multimap.insert(*free_region);

        return free_region;
    }


    /// Precondition: free_region.size >= size.
    RegionMetadata * allocateFromFreeRegion(RegionMetadata & free_region, size_t size)
    {
        ++allocations;
        allocated_bytes += size;

        if (free_region.size == size)
        {
            total_allocated_size += size;
            size_multimap.erase(size_multimap.iterator_to(free_region));
            return &free_region;
        }

        RegionMetadata * allocated_region = RegionMetadata::create();
        total_allocated_size += size;

        allocated_region->ptr = free_region.ptr;
        allocated_region->chunk = free_region.chunk;
        allocated_region->size = size;

        size_multimap.erase(size_multimap.iterator_to(free_region));
        free_region.size -= size;
        free_region.char_ptr += size;
        size_multimap.insert(free_region);

        adjacency_list.insert(adjacency_list.iterator_to(free_region), *allocated_region);
        return allocated_region;
    }


    /// Does not insert allocated region to key_map or lru_list. Caller must do it.
    RegionMetadata * allocate(size_t size)
    {
        size = roundUp(size, alignment);

        /// Look up to size multimap to find free region of specified size.
        auto it = size_multimap.lower_bound(size, RegionCompareBySize());
        if (size_multimap.end() != it)
        {
            return allocateFromFreeRegion(*it, size);
        }

        /// If nothing was found and total size of allocated chunks plus required size is lower than maximum,
        ///  allocate a new chunk.
        size_t page_size = static_cast<size_t>(::getPageSize());
        size_t required_chunk_size = std::max(min_chunk_size, roundUp(size, page_size));
        if (total_chunks_size + required_chunk_size <= max_total_size)
        {
            /// Create free region spanning through chunk.
            RegionMetadata * free_region = addNewChunk(required_chunk_size);
            return allocateFromFreeRegion(*free_region, size);
        }

//        std::cerr << "Requested size: " << size << "\n";

        /// Evict something from cache and continue.
        while (true)
        {
            RegionMetadata * res = evictSome(size);

            /// Nothing to evict. All cache is full and in use - cannot allocate memory.
            if (!res)
                return nullptr;

            /// Not enough. Evict more.
            if (res->size < size)
                continue;

            return allocateFromFreeRegion(*res, size);
        }
    }


public:
    explicit ArrayCache(size_t max_total_size_) : max_total_size(max_total_size_)
    {
    }

    ~ArrayCache()
    {
        std::lock_guard cache_lock(mutex);

        key_map.clear();
        lru_list.clear();
        size_multimap.clear();
        adjacency_list.clear_and_dispose([](RegionMetadata * elem) { elem->destroy(); });
    }


    /// If the value for the key is in the cache, returns it.
    ///
    /// If it is not, calls 'get_size' to obtain required size of storage for key,
    ///  then allocates storage and call 'initialize' for necessary initialization before data from cache could be used.
    ///
    /// Only one of several concurrent threads calling this method will call get_size or initialize,
    /// others will wait for that call to complete and will use its result (this helps prevent cache stampede).
    ///
    /// Exceptions occurring in callbacks will be propagated to the caller.
    /// Another thread from the set of concurrent threads will then try to call its callbacks etc.
    ///
    /// Returns cached value wrapped by holder, preventing cache entry from eviction.
    /// Also could return a bool indicating whether the value was produced during this call.
    template <typename GetSizeFunc, typename InitializeFunc>
    HolderPtr getOrSet(const Key & key, GetSizeFunc && get_size, InitializeFunc && initialize, bool * was_calculated)
    {
        InsertTokenHolder token_holder;
        {
            std::lock_guard cache_lock(mutex);

            auto it = key_map.find(key, RegionCompareByKey());
            if (key_map.end() != it)
            {
                ++hits;
                if (was_calculated)
                    *was_calculated = false;

                return std::make_shared<Holder>(*this, *it);
            }

            auto & token = insert_tokens[key];
            if (!token)
                token = std::make_shared<InsertToken>(*this);

            token_holder.acquire(&key, token, cache_lock);
        }

        InsertToken * token = token_holder.token.get();

        std::lock_guard token_lock(token->mutex);

        token_holder.cleaned_up = token->cleaned_up;

        if (token->value)
        {
            /// Another thread already produced the value while we waited for token->mutex.
            ++hits;
            ++concurrent_hits;
            if (was_calculated)
                *was_calculated = false;

            return token->value;
        }

        ++misses;

        size_t size = get_size();

        RegionMetadata * region;
        {
            std::lock_guard cache_lock(mutex);
            region = allocate(size);
        }

        /// Cannot allocate memory.
        if (!region)
            return {};

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
                    freeRegion(*region);
                }
                throw;
            }
        }

        std::lock_guard cache_lock(mutex);

        try
        {
            token->value = std::make_shared<Holder>(*this, *region);

            /// Insert the new value only if the token is still in present in insert_tokens.
            /// (The token may be absent because of a concurrent reset() call).
            auto token_it = insert_tokens.find(key);
            if (token_it != insert_tokens.end() && token_it->second.get() == token)
            {
                key_map.insert(*region);
            }

            if (!token->cleaned_up)
                token_holder.cleanup(token_lock, cache_lock);

            if (was_calculated)
                *was_calculated = true;

            return token->value;
        }
        catch (...)
        {
            if (region->KeyMapHook::is_linked())
                key_map.erase(key_map.iterator_to(*region));

            freeRegion(*region);
            throw;
        }
    }


    struct Statistics
    {
        size_t total_chunks_size = 0;
        size_t total_allocated_size = 0;
        size_t total_size_currently_initialized = 0;
        size_t total_size_in_use = 0;

        size_t num_chunks = 0;
        size_t num_regions = 0;
        size_t num_free_regions = 0;
        size_t num_regions_in_use = 0;
        size_t num_keyed_regions = 0;

        size_t hits = 0;
        size_t concurrent_hits = 0;
        size_t misses = 0;

        size_t allocations = 0;
        size_t allocated_bytes = 0;
        size_t evictions = 0;
        size_t evicted_bytes = 0;
        size_t secondary_evictions = 0;
    };

    Statistics getStatistics() const
    {
        std::lock_guard cache_lock(mutex);
        Statistics res;

        res.total_chunks_size = total_chunks_size;
        res.total_allocated_size = total_allocated_size;
        res.total_size_currently_initialized = total_size_currently_initialized.load(std::memory_order_relaxed);
        res.total_size_in_use = total_size_in_use;

        res.num_chunks = chunks.size();
        res.num_regions = adjacency_list.size();
        res.num_free_regions = size_multimap.size();
        res.num_regions_in_use = adjacency_list.size() - size_multimap.size() - lru_list.size();
        res.num_keyed_regions = key_map.size();

        res.hits = hits.load(std::memory_order_relaxed);
        res.concurrent_hits = concurrent_hits.load(std::memory_order_relaxed);
        res.misses = misses.load(std::memory_order_relaxed);

        res.allocations = allocations;
        res.allocated_bytes = allocated_bytes;
        res.evictions = evictions;
        res.evicted_bytes = evicted_bytes;
        res.secondary_evictions = secondary_evictions;

        return res;
    }
};

template <typename Key, typename Payload> constexpr size_t ArrayCache<Key, Payload>::min_chunk_size;
