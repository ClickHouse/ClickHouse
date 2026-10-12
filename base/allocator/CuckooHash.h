#pragma once

/// Cuckoo hash table (jemalloc: `ckh.h`, `ckh.c`).
///
/// (2^n,2) cuckoo hashing: every bucket has 2^`LOG2_CUCKOO_HASH_BUCKET_CELLS` cells (one bucket per cache line) and every key
/// has two candidate buckets given by the two words of its hash. The table only stores pointers to the keys and
/// the values and does no lifetime management.
///
/// The port is bit-exact: the same LCG (seeded with 42, private to every table) decides the starting cell of a bucket
/// probe and the victim of an eviction, the tables grow and shrink at the same thresholds and are rebuilt in the same
/// order, so that the cell occupied by every key and therefore the iteration order are those of jemalloc. The
/// iteration order is observable in the profiler (which `thread_context` states get snapshotted during a dump).
///
/// The table memory is obtained from the `Allocator` policy:
///
///     struct Allocator
///     {
///         /// Zeroed memory of `usable_size` bytes aligned to `alignment`, or null.
///         static void * allocate(ThreadState & tsd, size_t usize, size_t alignment);
///         static void deallocate(ThreadState & tsd, void * ptr);
///     };
///
/// jemalloc uses `ipallocztm(tsdn, usize, CACHELINE, zero = true, tcache = NULL, is_internal = true,
/// arena_ichoose(tsd, NULL))` and `idalloctm(tsdn, ptr, NULL, NULL, is_internal = true, slow_path = true)`; the
/// profiler provides a policy doing exactly that. `usable_size` is computed here exactly like jemalloc
/// (`sz_sa2u(sizeof(ckhc_t) << log2_cells, CACHE_LINE)`); 0 or more than `SIZE_CLASS_LARGE_MAX_CLASS` fails the operation before
/// calling the allocator.

#include <allocator/Common.h>
#include <allocator/SizeClasses.h>

namespace jemalloc
{

class ThreadState;

/// There are 2^LOG2_CUCKOO_HASH_BUCKET_CELLS cells in each hash table bucket. Try to fit one bucket per L1 cache line.
/// jemalloc: LG_CKH_BUCKET_CELLS
inline constexpr unsigned LOG2_CUCKOO_HASH_BUCKET_CELLS = LOG2_CACHE_LINE - LG_SIZEOF_PTR - 1;
static_assert(LOG2_CUCKOO_HASH_BUCKET_CELLS > 0);

/// jemalloc: ckh_hash_t
using CuckooHashFunction = void (*)(const void * key, size_t result_hash[2]);
/// jemalloc: ckh_keycomp_t
using CuckooKeyCompare = bool (*)(const void * k1, const void * k2);

/// Hash table cell. jemalloc: ckhc_t
struct CuckooHashCell
{
    const void * key;
    const void * data;
};

static_assert(sizeof(CuckooHashCell) == 16);

/// The data and the non-allocating operations of the table (the layout of `ckh_t` without `CKH_COUNT`).
class CuckooHashBase
{
public:
    /// Not found (`SIZE_T_MAX`).
    static constexpr size_t NOT_FOUND = SIZE_MAX;

    /// Get the number of elements in the set.
    /// jemalloc: ckh_count
    size_t count() const { return count_; }

    /// To iterate over the elements in the table, initialize `*table_idx` to 0 and call this function until it returns
    /// true. Each call that returns false updates `*key` and `*data` to the next element in the table, assuming the
    /// pointers are non-null.
    /// jemalloc: ckh_iter
    bool iterate(size_t * table_idx, void ** key, void ** data) const;

    /// Returns true if not found. `key` or `data` may be null.
    /// jemalloc: ckh_search
    bool search(const void * search_key, void ** key, void ** data) const;

    /// --- Introspection (tests and debugging) ---

    unsigned log2MinBuckets() const { return log2_min_buckets; }
    unsigned log2CurrentBuckets() const { return log2_current_buckets; }
    size_t numCells() const { return size_t(1) << (log2_current_buckets + LOG2_CUCKOO_HASH_BUCKET_CELLS); }
    const CuckooHashCell * cells() const { return table; }
    uint64_t prngState() const { return prng_state; }

protected:
    /// Search the table for the key and return the cell number if found; `NOT_FOUND` otherwise.
    /// jemalloc: ckh_isearch
    size_t searchInternal(const void * key) const;

    /// jemalloc: ckh_bucket_search
    size_t bucketSearch(size_t bucket, const void * key) const;

    /// Returns true if the bucket is full.
    /// jemalloc: ckh_try_bucket_insert
    bool tryBucketInsert(size_t bucket, const void * key, const void * data);

    /// No space is available in the bucket. Randomly evict an item, then try to find an alternate location for that
    /// item. Iteratively repeat this eviction/relocation procedure until either success or detection of an
    /// eviction/relocation bucket cycle; in the latter case returns true with the item that is left over in
    /// `*argument_key`/`*argument_data`.
    /// jemalloc: ckh_evict_reloc_insert
    bool evictRelocateInsert(size_t argument_bucket, const void ** argument_key, const void ** argument_data);

    /// Returns true on failure, with the item that could not be placed in `*argument_key`/`*argument_data`.
    /// jemalloc: ckh_try_insert
    bool tryInsert(const void ** argument_key, const void ** argument_data);

    /// Try to rebuild the hash table from scratch by inserting all items from the old table `old_table` into the new
    /// (current) one. Returns true on failure.
    /// jemalloc: ckh_rebuild
    bool rebuild(const CuckooHashCell * old_table);

    /// The size of a table of 2^`log2_cells` cells, computed exactly as jemalloc does; 0 means "fail".
    static size_t tableUsableSize(unsigned log2_cells)
    {
        size_t usable_size = size_classes::alignedSizeToUsableSize(sizeof(CuckooHashCell) << log2_cells, CACHE_LINE);
        if (ALLOCATOR_UNLIKELY(usable_size == 0 || usable_size > SIZE_CLASS_LARGE_MAX_CLASS))
            return 0;
        return usable_size;
    }

    /// Initializes everything except the table; returns `log2_min_cells`. The first part of `ckh_new`.
    unsigned initFields(size_t min_items, CuckooHashFunction hash_function, CuckooKeyCompare key_compare_function);

    /// Clears a cell after a successful `searchInternal`; returns true if the table should be shrunk.
    /// The first part of `ckh_remove`.
    bool removeCell(size_t cell, void ** key, void ** data);

    /// Used for pseudo-random number generation.
    uint64_t prng_state;
    /// Total number of items.
    size_t count_;
    /// Minimum and current number of hash table buckets. There are 2^LOG2_CUCKOO_HASH_BUCKET_CELLS cells per bucket.
    unsigned log2_min_buckets;
    unsigned log2_current_buckets;
    /// Hash and comparison functions.
    CuckooHashFunction hash;
    CuckooKeyCompare key_compare;
    /// Hash table with 2^lg_curbuckets buckets.
    CuckooHashCell * table;
};

static_assert(sizeof(CuckooHashBase) == 48, "Must have the size of ckh_t");

/// jemalloc: ckh_t
template <typename Allocator>
class CuckooHash : public CuckooHashBase
{
public:
    /// Lifetime management. `min_items` is the initial capacity. Returns true on error (OOM).
    /// jemalloc: ckh_new
    bool init(ThreadState & thread_state, size_t min_items, CuckooHashFunction hash_function, CuckooKeyCompare key_compare_function)
    {
        unsigned log2_min_cells = initFields(min_items, hash_function, key_compare_function);
        size_t usable_size = tableUsableSize(log2_min_cells);
        if (ALLOCATOR_UNLIKELY(usable_size == 0))
            return true;
        table = static_cast<CuckooHashCell *>(Allocator::allocate(thread_state, usable_size, CACHE_LINE));
        return table == nullptr;
    }

    /// jemalloc: ckh_delete
    void destroy(ThreadState & thread_state)
    {
        Allocator::deallocate(thread_state, table);
        if constexpr (config::debug)
            std::memset(static_cast<void *>(this), 0x5a, sizeof(CuckooHashBase)); /// JEMALLOC_FREE_JUNK
    }

    /// The key must not be present. Returns true on error (OOM).
    /// jemalloc: ckh_insert
    bool insert(ThreadState & thread_state, const void * key, const void * data)
    {
        ALLOCATOR_ASSERT(search(key, nullptr, nullptr));

        while (tryInsert(&key, &data))
        {
            /// Note that the item retried after growing is the one left over from the eviction chain.
            if (grow(thread_state))
                return true;
        }
        return false;
    }

    /// Returns true if not found. `key` or `data` may be null.
    /// jemalloc: ckh_remove
    bool remove(ThreadState & thread_state, const void * search_key, void ** key, void ** data)
    {
        size_t cell = searchInternal(search_key);
        if (cell == NOT_FOUND)
            return true;
        if (removeCell(cell, key, data))
        {
            /// Ignore error due to OOM.
            shrink(thread_state);
        }
        return false;
    }

private:
    /// Returns true on error (OOM).
    /// jemalloc: ckh_grow
    bool grow(ThreadState & thread_state)
    {
        /// It is possible (though unlikely, given well behaved hashes) that the table will have to be doubled more
        /// than once in order to create a usable table.
        unsigned log2_previous_buckets = log2_current_buckets;
        unsigned log2_current_cells = log2_current_buckets + LOG2_CUCKOO_HASH_BUCKET_CELLS;
        while (true)
        {
            ++log2_current_cells;
            size_t usable_size = tableUsableSize(log2_current_cells);
            if (ALLOCATOR_UNLIKELY(usable_size == 0))
                return true;
            auto * new_table = static_cast<CuckooHashCell *>(Allocator::allocate(thread_state, usable_size, CACHE_LINE));
            if (new_table == nullptr)
                return true;

            /// Swap in the new table.
            CuckooHashCell * old_table = table;
            table = new_table;
            log2_current_buckets = log2_current_cells - LOG2_CUCKOO_HASH_BUCKET_CELLS;

            if (!rebuild(old_table))
            {
                Allocator::deallocate(thread_state, old_table);
                return false;
            }

            /// Rebuilding failed, so back out the partially rebuilt table.
            Allocator::deallocate(thread_state, table);
            table = old_table;
            log2_current_buckets = log2_previous_buckets;
        }
    }

    /// jemalloc: ckh_shrink
    void shrink(ThreadState & thread_state)
    {
        /// It is possible (though unlikely, given well behaved hashes) that the table rebuild will fail.
        unsigned log2_previous_buckets = log2_current_buckets;
        unsigned log2_current_cells = log2_current_buckets + LOG2_CUCKOO_HASH_BUCKET_CELLS - 1;
        size_t usable_size = tableUsableSize(log2_current_cells);
        if (ALLOCATOR_UNLIKELY(usable_size == 0))
            return;
        auto * new_table = static_cast<CuckooHashCell *>(Allocator::allocate(thread_state, usable_size, CACHE_LINE));
        if (new_table == nullptr)
        {
            /// An OOM error isn't worth propagating, since it doesn't prevent this or future operations from
            /// proceeding.
            return;
        }

        /// Swap in the new table.
        CuckooHashCell * old_table = table;
        table = new_table;
        log2_current_buckets = log2_current_cells - LOG2_CUCKOO_HASH_BUCKET_CELLS;

        if (!rebuild(old_table))
        {
            Allocator::deallocate(thread_state, old_table);
            return;
        }

        /// Rebuilding failed, so back out the partially rebuilt table.
        Allocator::deallocate(thread_state, table);
        table = old_table;
        log2_current_buckets = log2_previous_buckets;
    }
};

/// Some useful hash and comparison functions for strings and pointers.
/// jemalloc: ckh_string_hash
void cuckooHashStringHash(const void * key, size_t result_hash[2]);
/// jemalloc: ckh_string_keycomp
bool cuckooHashStringKeyCompare(const void * k1, const void * k2);
/// jemalloc: ckh_pointer_hash
void cuckooHashPointerHash(const void * key, size_t result_hash[2]);
/// jemalloc: ckh_pointer_keycomp
bool cuckooHashPointerKeyCompare(const void * k1, const void * k2);

}
