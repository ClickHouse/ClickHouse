#pragma once

/// Cuckoo hash table (jemalloc: `ckh.h`, `ckh.c`).
///
/// (2^n,2) cuckoo hashing: every bucket has 2^`LG_CKH_BUCKET_CELLS` cells (one bucket per cache line) and every key
/// has two candidate buckets given by the two words of its hash. The table only stores pointers to the keys and
/// the values and does no lifetime management.
///
/// The port is bit-exact: the same LCG (seeded with 42, private to every table) decides the starting cell of a bucket
/// probe and the victim of an eviction, the tables grow and shrink at the same thresholds and are rebuilt in the same
/// order, so that the cell occupied by every key and therefore the iteration order are those of jemalloc. The
/// iteration order is observable in the profiler (which `tctx` states get snapshotted during a dump).
///
/// The table memory is obtained from the `Allocator` policy:
///
///     struct Allocator
///     {
///         /// Zeroed memory of `usize` bytes aligned to `alignment`, or null.
///         static void * allocate(ThreadState & tsd, size_t usize, size_t alignment);
///         static void deallocate(ThreadState & tsd, void * ptr);
///     };
///
/// jemalloc uses `ipallocztm(tsdn, usize, CACHELINE, zero = true, tcache = NULL, is_internal = true,
/// arena_ichoose(tsd, NULL))` and `idalloctm(tsdn, ptr, NULL, NULL, is_internal = true, slow_path = true)`; the
/// profiler provides a policy doing exactly that. `usize` is computed here exactly like jemalloc
/// (`sz_sa2u(sizeof(ckhc_t) << lg_cells, CACHELINE)`); 0 or more than `SC_LARGE_MAXCLASS` fails the operation before
/// calling the allocator.

#include <allocator/Common.h>
#include <allocator/SizeClasses.h>

namespace jemalloc
{

class ThreadState;

/// There are 2^LG_CKH_BUCKET_CELLS cells in each hash table bucket. Try to fit one bucket per L1 cache line.
/// jemalloc: LG_CKH_BUCKET_CELLS
inline constexpr unsigned LG_CKH_BUCKET_CELLS = LG_CACHELINE - LG_SIZEOF_PTR - 1;
static_assert(LG_CKH_BUCKET_CELLS > 0);

/// jemalloc: ckh_hash_t
using CuckooHashFunction = void (*)(const void * key, size_t r_hash[2]);
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

    /// To iterate over the elements in the table, initialize `*tabind` to 0 and call this function until it returns
    /// true. Each call that returns false updates `*key` and `*data` to the next element in the table, assuming the
    /// pointers are non-null.
    /// jemalloc: ckh_iter
    bool iter(size_t * tabind, void ** key, void ** data) const;

    /// Returns true if not found. `key` or `data` may be null.
    /// jemalloc: ckh_search
    bool search(const void * searchkey, void ** key, void ** data) const;

    /// --- Introspection (tests and debugging) ---

    unsigned lgMinBuckets() const { return lg_minbuckets; }
    unsigned lgCurBuckets() const { return lg_curbuckets; }
    size_t numCells() const { return size_t(1) << (lg_curbuckets + LG_CKH_BUCKET_CELLS); }
    const CuckooHashCell * cells() const { return tab; }
    uint64_t prngState() const { return prng_state; }

protected:
    /// Search the table for the key and return the cell number if found; `NOT_FOUND` otherwise.
    /// jemalloc: ckh_isearch
    size_t isearch(const void * key) const;

    /// jemalloc: ckh_bucket_search
    size_t bucketSearch(size_t bucket, const void * key) const;

    /// Returns true if the bucket is full.
    /// jemalloc: ckh_try_bucket_insert
    bool tryBucketInsert(size_t bucket, const void * key, const void * data);

    /// No space is available in the bucket. Randomly evict an item, then try to find an alternate location for that
    /// item. Iteratively repeat this eviction/relocation procedure until either success or detection of an
    /// eviction/relocation bucket cycle; in the latter case returns true with the item that is left over in
    /// `*argkey`/`*argdata`.
    /// jemalloc: ckh_evict_reloc_insert
    bool evictRelocInsert(size_t argbucket, const void ** argkey, const void ** argdata);

    /// Returns true on failure, with the item that could not be placed in `*argkey`/`*argdata`.
    /// jemalloc: ckh_try_insert
    bool tryInsert(const void ** argkey, const void ** argdata);

    /// Try to rebuild the hash table from scratch by inserting all items from the old table `old_tab` into the new
    /// (current) one. Returns true on failure.
    /// jemalloc: ckh_rebuild
    bool rebuild(const CuckooHashCell * old_tab);

    /// The size of a table of 2^`lg_cells` cells, computed exactly as jemalloc does; 0 means "fail".
    static size_t tableUsize(unsigned lg_cells)
    {
        size_t usize = sz::sa2u(sizeof(CuckooHashCell) << lg_cells, CACHELINE);
        if (JE_UNLIKELY(usize == 0 || usize > SC_LARGE_MAXCLASS))
            return 0;
        return usize;
    }

    /// Initializes everything except the table; returns `lg_mincells`. The first part of `ckh_new`.
    unsigned initFields(size_t minitems, CuckooHashFunction hash_function, CuckooKeyCompare keycomp_function);

    /// Clears a cell after a successful `isearch`; returns true if the table should be shrunk.
    /// The first part of `ckh_remove`.
    bool removeCell(size_t cell, void ** key, void ** data);

    /// Used for pseudo-random number generation.
    uint64_t prng_state;
    /// Total number of items.
    size_t count_;
    /// Minimum and current number of hash table buckets. There are 2^LG_CKH_BUCKET_CELLS cells per bucket.
    unsigned lg_minbuckets;
    unsigned lg_curbuckets;
    /// Hash and comparison functions.
    CuckooHashFunction hash;
    CuckooKeyCompare keycomp;
    /// Hash table with 2^lg_curbuckets buckets.
    CuckooHashCell * tab;
};

static_assert(sizeof(CuckooHashBase) == 48, "Must have the size of ckh_t");

/// jemalloc: ckh_t
template <typename Allocator>
class CuckooHash : public CuckooHashBase
{
public:
    /// Lifetime management. `minitems` is the initial capacity. Returns true on error (OOM).
    /// jemalloc: ckh_new
    bool init(ThreadState & tsd, size_t minitems, CuckooHashFunction hash_function, CuckooKeyCompare keycomp_function)
    {
        unsigned lg_mincells = initFields(minitems, hash_function, keycomp_function);
        size_t usize = tableUsize(lg_mincells);
        if (JE_UNLIKELY(usize == 0))
            return true;
        tab = static_cast<CuckooHashCell *>(Allocator::allocate(tsd, usize, CACHELINE));
        return tab == nullptr;
    }

    /// jemalloc: ckh_delete
    void destroy(ThreadState & tsd)
    {
        Allocator::deallocate(tsd, tab);
        if constexpr (config::debug)
            std::memset(static_cast<void *>(this), 0x5a, sizeof(CuckooHashBase)); /// JEMALLOC_FREE_JUNK
    }

    /// The key must not be present. Returns true on error (OOM).
    /// jemalloc: ckh_insert
    bool insert(ThreadState & tsd, const void * key, const void * data)
    {
        JE_ASSERT(search(key, nullptr, nullptr));

        while (tryInsert(&key, &data))
        {
            /// Note that the item retried after growing is the one left over from the eviction chain.
            if (grow(tsd))
                return true;
        }
        return false;
    }

    /// Returns true if not found. `key` or `data` may be null.
    /// jemalloc: ckh_remove
    bool remove(ThreadState & tsd, const void * searchkey, void ** key, void ** data)
    {
        size_t cell = isearch(searchkey);
        if (cell == NOT_FOUND)
            return true;
        if (removeCell(cell, key, data))
        {
            /// Ignore error due to OOM.
            shrink(tsd);
        }
        return false;
    }

private:
    /// Returns true on error (OOM).
    /// jemalloc: ckh_grow
    bool grow(ThreadState & tsd)
    {
        /// It is possible (though unlikely, given well behaved hashes) that the table will have to be doubled more
        /// than once in order to create a usable table.
        unsigned lg_prevbuckets = lg_curbuckets;
        unsigned lg_curcells = lg_curbuckets + LG_CKH_BUCKET_CELLS;
        while (true)
        {
            ++lg_curcells;
            size_t usize = tableUsize(lg_curcells);
            if (JE_UNLIKELY(usize == 0))
                return true;
            auto * new_tab = static_cast<CuckooHashCell *>(Allocator::allocate(tsd, usize, CACHELINE));
            if (new_tab == nullptr)
                return true;

            /// Swap in the new table.
            CuckooHashCell * old_tab = tab;
            tab = new_tab;
            lg_curbuckets = lg_curcells - LG_CKH_BUCKET_CELLS;

            if (!rebuild(old_tab))
            {
                Allocator::deallocate(tsd, old_tab);
                return false;
            }

            /// Rebuilding failed, so back out the partially rebuilt table.
            Allocator::deallocate(tsd, tab);
            tab = old_tab;
            lg_curbuckets = lg_prevbuckets;
        }
    }

    /// jemalloc: ckh_shrink
    void shrink(ThreadState & tsd)
    {
        /// It is possible (though unlikely, given well behaved hashes) that the table rebuild will fail.
        unsigned lg_prevbuckets = lg_curbuckets;
        unsigned lg_curcells = lg_curbuckets + LG_CKH_BUCKET_CELLS - 1;
        size_t usize = tableUsize(lg_curcells);
        if (JE_UNLIKELY(usize == 0))
            return;
        auto * new_tab = static_cast<CuckooHashCell *>(Allocator::allocate(tsd, usize, CACHELINE));
        if (new_tab == nullptr)
        {
            /// An OOM error isn't worth propagating, since it doesn't prevent this or future operations from
            /// proceeding.
            return;
        }

        /// Swap in the new table.
        CuckooHashCell * old_tab = tab;
        tab = new_tab;
        lg_curbuckets = lg_curcells - LG_CKH_BUCKET_CELLS;

        if (!rebuild(old_tab))
        {
            Allocator::deallocate(tsd, old_tab);
            return;
        }

        /// Rebuilding failed, so back out the partially rebuilt table.
        Allocator::deallocate(tsd, tab);
        tab = old_tab;
        lg_curbuckets = lg_prevbuckets;
    }
};

/// Some useful hash and comparison functions for strings and pointers.
/// jemalloc: ckh_string_hash
void ckhStringHash(const void * key, size_t r_hash[2]);
/// jemalloc: ckh_string_keycomp
bool ckhStringKeycomp(const void * k1, const void * k2);
/// jemalloc: ckh_pointer_hash
void ckhPointerHash(const void * key, size_t r_hash[2]);
/// jemalloc: ckh_pointer_keycomp
bool ckhPointerKeycomp(const void * k1, const void * k2);

}
