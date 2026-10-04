/// Implementation of (2^1+,2) cuckoo hashing, where 2^1+ indicates that each hash bucket contains 2^n cells, for
/// n >= 1, and 2 indicates that two hash functions are employed. The original cuckoo hashing algorithm was described
/// in:
///
///   Pagh, R., F.F. Rodler (2004) Cuckoo Hashing. Journal of Algorithms 51(2):122-144.
///
/// Generalization of cuckoo hashing was discussed in:
///
///   Erlingsson, U., M. Manasse, F. McSherry (2006) A cool and practical alternative to traditional hash tables. In
///   Proceedings of the 7th Workshop on Distributed Data and Structures (WDAS'06), Santa Clara, CA, January 2006.
///
/// This implementation uses precisely two hash functions because that is the fewest that can work. The number of
/// cells per bucket is chosen such that a bucket fits in one cache line, so on 64-bit systems we use (4,2) cuckoo
/// hashing.

#include <allocator/CuckooHash.h>

#include <allocator/Hash.h>
#include <allocator/Prng.h>

#include <cstring>

namespace jemalloc
{

namespace
{

constexpr size_t BUCKET_CELLS = size_t(1) << LG_CKH_BUCKET_CELLS;

}

/// jemalloc: ckh_bucket_search
size_t CuckooHashBase::bucketSearch(size_t bucket, const void * key) const
{
    for (unsigned i = 0; i < BUCKET_CELLS; ++i)
    {
        const CuckooHashCell * cell = &tab[(bucket << LG_CKH_BUCKET_CELLS) + i];
        if (cell->key != nullptr && keycomp(key, cell->key))
            return (bucket << LG_CKH_BUCKET_CELLS) + i;
    }
    return NOT_FOUND;
}

/// jemalloc: ckh_isearch
size_t CuckooHashBase::isearch(const void * key) const
{
    size_t hashes[2];
    hash(key, hashes);

    /// Search the primary bucket.
    size_t bucket = hashes[0] & ((size_t(1) << lg_curbuckets) - 1);
    size_t cell = bucketSearch(bucket, key);
    if (cell != NOT_FOUND)
        return cell;

    /// Search the secondary bucket.
    bucket = hashes[1] & ((size_t(1) << lg_curbuckets) - 1);
    return bucketSearch(bucket, key);
}

/// jemalloc: ckh_try_bucket_insert
bool CuckooHashBase::tryBucketInsert(size_t bucket, const void * key, const void * data)
{
    /// Cycle through the cells in the bucket, starting at a random position. The randomness avoids worst-case search
    /// overhead as buckets fill up.
    auto offset = static_cast<unsigned>(prngLgRangeU64(prng_state, LG_CKH_BUCKET_CELLS));
    for (unsigned i = 0; i < BUCKET_CELLS; ++i)
    {
        CuckooHashCell * cell = &tab[(bucket << LG_CKH_BUCKET_CELLS) + ((i + offset) & (BUCKET_CELLS - 1))];
        if (cell->key == nullptr)
        {
            cell->key = key;
            cell->data = data;
            ++count_;
            return false;
        }
    }
    return true;
}

/// jemalloc: ckh_evict_reloc_insert
bool CuckooHashBase::evictRelocInsert(size_t argbucket, const void ** argkey, const void ** argdata)
{
    size_t bucket = argbucket;
    const void * key = *argkey;
    const void * data = *argdata;
    while (true)
    {
        /// Choose a random item within the bucket to evict. This is critical to correct function, because without
        /// (eventually) evicting all items within a bucket during iteration, it would be possible to get stuck in an
        /// infinite loop if there were an item for which both hashes indicated the same bucket.
        auto i = static_cast<unsigned>(prngLgRangeU64(prng_state, LG_CKH_BUCKET_CELLS));
        CuckooHashCell * cell = &tab[(bucket << LG_CKH_BUCKET_CELLS) + i];
        JE_ASSERT(cell->key != nullptr);

        /// Swap cell->{key,data} and {key,data} (evict).
        const void * tkey = cell->key;
        const void * tdata = cell->data;
        cell->key = key;
        cell->data = data;
        key = tkey;
        data = tdata;

        /// Find the alternate bucket for the evicted item.
        size_t hashes[2];
        hash(key, hashes);
        size_t tbucket = hashes[1] & ((size_t(1) << lg_curbuckets) - 1);
        if (tbucket == bucket)
        {
            tbucket = hashes[0] & ((size_t(1) << lg_curbuckets) - 1);
            /// It may be that (tbucket == bucket) still, if the item's hashes both indicate this bucket. However, we
            /// are guaranteed to eventually escape this bucket during iteration, assuming pseudo-random item
            /// selection: either this bucket == argbucket, so we will quickly detect an eviction cycle and
            /// terminate, or an item was evicted to this bucket from another, which means that at least one item in
            /// this bucket has hashes that indicate distinct buckets.
        }
        /// Check for a cycle.
        if (tbucket == argbucket)
        {
            *argkey = key;
            *argdata = data;
            return true;
        }

        bucket = tbucket;
        if (!tryBucketInsert(bucket, key, data))
            return false;
    }
}

/// jemalloc: ckh_try_insert
bool CuckooHashBase::tryInsert(const void ** argkey, const void ** argdata)
{
    const void * key = *argkey;
    const void * data = *argdata;

    size_t hashes[2];
    hash(key, hashes);

    /// Try to insert in the primary bucket.
    size_t bucket = hashes[0] & ((size_t(1) << lg_curbuckets) - 1);
    if (!tryBucketInsert(bucket, key, data))
        return false;

    /// Try to insert in the secondary bucket.
    bucket = hashes[1] & ((size_t(1) << lg_curbuckets) - 1);
    if (!tryBucketInsert(bucket, key, data))
        return false;

    /// Try to find a place for this item via iterative eviction/relocation.
    return evictRelocInsert(bucket, argkey, argdata);
}

/// jemalloc: ckh_rebuild
bool CuckooHashBase::rebuild(const CuckooHashCell * old_tab)
{
    size_t total = count_;
    count_ = 0;
    for (size_t i = 0, nins = 0; nins < total; ++i)
    {
        if (old_tab[i].key != nullptr)
        {
            const void * key = old_tab[i].key;
            const void * data = old_tab[i].data;
            if (tryInsert(&key, &data))
            {
                count_ = total;
                return true;
            }
            ++nins;
        }
    }
    return false;
}

/// The first part of jemalloc's `ckh_new`.
unsigned CuckooHashBase::initFields(size_t minitems, CuckooHashFunction hash_function, CuckooKeyCompare keycomp_function)
{
    JE_ASSERT(minitems > 0);
    JE_ASSERT(hash_function != nullptr);
    JE_ASSERT(keycomp_function != nullptr);

    prng_state = 42; /// Value doesn't really matter.
    count_ = 0;

    /// Find the minimum power of 2 that is large enough to fit minitems entries. We are using (2+,2) cuckoo hashing,
    /// which has an expected maximum load factor of at least ~0.86, so 0.75 is a conservative load factor that will
    /// typically allow mincells items to fit without ever growing the table.
    size_t mincells = ((minitems + (3 - (minitems % 3))) / 3) << 2;
    unsigned lg_mincells = LG_CKH_BUCKET_CELLS;
    while ((size_t(1) << lg_mincells) < mincells)
        ++lg_mincells;
    lg_minbuckets = lg_mincells - LG_CKH_BUCKET_CELLS;
    lg_curbuckets = lg_mincells - LG_CKH_BUCKET_CELLS;
    hash = hash_function;
    keycomp = keycomp_function;
    tab = nullptr;
    return lg_mincells;
}

/// jemalloc: ckh_iter
bool CuckooHashBase::iter(size_t * tabind, void ** key, void ** data) const
{
    for (size_t i = *tabind, ncells = numCells(); i < ncells; ++i)
    {
        if (tab[i].key != nullptr)
        {
            if (key != nullptr)
                *key = const_cast<void *>(tab[i].key);
            if (data != nullptr)
                *data = const_cast<void *>(tab[i].data);
            *tabind = i + 1;
            return false;
        }
    }
    return true;
}

/// The first part of jemalloc's `ckh_remove`.
bool CuckooHashBase::removeCell(size_t cell, void ** key, void ** data)
{
    if (key != nullptr)
        *key = const_cast<void *>(tab[cell].key);
    if (data != nullptr)
        *data = const_cast<void *>(tab[cell].data);
    tab[cell].key = nullptr;
    tab[cell].data = nullptr; /// Not necessary.

    --count_;
    /// Try to halve the table if it is less than 1/4 full.
    return count_ < (size_t(1) << (lg_curbuckets + LG_CKH_BUCKET_CELLS - 2)) && lg_curbuckets > lg_minbuckets;
}

/// jemalloc: ckh_search
bool CuckooHashBase::search(const void * searchkey, void ** key, void ** data) const
{
    size_t cell = isearch(searchkey);
    if (cell == NOT_FOUND)
        return true;
    if (key != nullptr)
        *key = const_cast<void *>(tab[cell].key);
    if (data != nullptr)
        *data = const_cast<void *>(tab[cell].data);
    return false;
}

/// jemalloc: ckh_string_hash
void ckhStringHash(const void * key, size_t r_hash[2])
{
    hash::hash(key, std::strlen(static_cast<const char *>(key)), 0x94122f33U, r_hash);
}

/// jemalloc: ckh_string_keycomp
bool ckhStringKeycomp(const void * k1, const void * k2)
{
    JE_ASSERT(k1 != nullptr);
    JE_ASSERT(k2 != nullptr);
    return std::strcmp(static_cast<const char *>(k1), static_cast<const char *>(k2)) == 0;
}

/// jemalloc: ckh_pointer_hash
void ckhPointerHash(const void * key, size_t r_hash[2])
{
    size_t i = reinterpret_cast<uintptr_t>(key);
    hash::hash(&i, sizeof(i), 0xd983396eU, r_hash);
}

/// jemalloc: ckh_pointer_keycomp
bool ckhPointerKeycomp(const void * k1, const void * k2)
{
    return k1 == k2;
}

}
