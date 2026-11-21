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
#include <allocator/PRNG.h>

#include <cstring>

namespace jemalloc
{

namespace
{

constexpr size_t BUCKET_CELLS = size_t(1) << LOG2_CUCKOO_HASH_BUCKET_CELLS;

}

/// jemalloc: ckh_bucket_search
size_t CuckooHashBase::bucketSearch(size_t bucket, const void * key) const
{
    for (unsigned i = 0; i < BUCKET_CELLS; ++i)
    {
        const CuckooHashCell * cell = &table[(bucket << LOG2_CUCKOO_HASH_BUCKET_CELLS) + i];
        if (cell->key != nullptr && key_compare(key, cell->key))
            return (bucket << LOG2_CUCKOO_HASH_BUCKET_CELLS) + i;
    }
    return NOT_FOUND;
}

/// jemalloc: ckh_isearch
size_t CuckooHashBase::searchInternal(const void * key) const
{
    size_t hashes[2];
    hash(key, hashes);

    /// Search the primary bucket.
    size_t bucket = hashes[0] & ((size_t(1) << log2_current_buckets) - 1);
    size_t cell = bucketSearch(bucket, key);
    if (cell != NOT_FOUND)
        return cell;

    /// Search the secondary bucket.
    bucket = hashes[1] & ((size_t(1) << log2_current_buckets) - 1);
    return bucketSearch(bucket, key);
}

/// jemalloc: ckh_try_bucket_insert
bool CuckooHashBase::tryBucketInsert(size_t bucket, const void * key, const void * data)
{
    /// Cycle through the cells in the bucket, starting at a random position. The randomness avoids worst-case search
    /// overhead as buckets fill up.
    auto offset = static_cast<unsigned>(prngLog2RangeU64(prng_state, LOG2_CUCKOO_HASH_BUCKET_CELLS));
    for (unsigned i = 0; i < BUCKET_CELLS; ++i)
    {
        CuckooHashCell * cell = &table[(bucket << LOG2_CUCKOO_HASH_BUCKET_CELLS) + ((i + offset) & (BUCKET_CELLS - 1))];
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
bool CuckooHashBase::evictRelocateInsert(size_t argument_bucket, const void ** argument_key, const void ** argument_data)
{
    size_t bucket = argument_bucket;
    const void * key = *argument_key;
    const void * data = *argument_data;
    while (true)
    {
        /// Choose a random item within the bucket to evict. This is critical to correct function, because without
        /// (eventually) evicting all items within a bucket during iteration, it would be possible to get stuck in an
        /// infinite loop if there were an item for which both hashes indicated the same bucket.
        auto i = static_cast<unsigned>(prngLog2RangeU64(prng_state, LOG2_CUCKOO_HASH_BUCKET_CELLS));
        CuckooHashCell * cell = &table[(bucket << LOG2_CUCKOO_HASH_BUCKET_CELLS) + i];
        ALLOCATOR_ASSERT(cell->key != nullptr);

        /// Swap cell->{key,data} and {key,data} (evict).
        const void * temporary_key = cell->key;
        const void * thread_data = cell->data;
        cell->key = key;
        cell->data = data;
        key = temporary_key;
        data = thread_data;

        /// Find the alternate bucket for the evicted item.
        size_t hashes[2];
        hash(key, hashes);
        size_t target_bucket = hashes[1] & ((size_t(1) << log2_current_buckets) - 1);
        if (target_bucket == bucket)
        {
            target_bucket = hashes[0] & ((size_t(1) << log2_current_buckets) - 1);
            /// It may be that (tbucket == bucket) still, if the item's hashes both indicate this bucket. However, we
            /// are guaranteed to eventually escape this bucket during iteration, assuming pseudo-random item
            /// selection: either this bucket == argbucket, so we will quickly detect an eviction cycle and
            /// terminate, or an item was evicted to this bucket from another, which means that at least one item in
            /// this bucket has hashes that indicate distinct buckets.
        }
        /// Check for a cycle.
        if (target_bucket == argument_bucket)
        {
            *argument_key = key;
            *argument_data = data;
            return true;
        }

        bucket = target_bucket;
        if (!tryBucketInsert(bucket, key, data))
            return false;
    }
}

/// jemalloc: ckh_try_insert
bool CuckooHashBase::tryInsert(const void ** argument_key, const void ** argument_data)
{
    const void * key = *argument_key;
    const void * data = *argument_data;

    size_t hashes[2];
    hash(key, hashes);

    /// Try to insert in the primary bucket.
    size_t bucket = hashes[0] & ((size_t(1) << log2_current_buckets) - 1);
    if (!tryBucketInsert(bucket, key, data))
        return false;

    /// Try to insert in the secondary bucket.
    bucket = hashes[1] & ((size_t(1) << log2_current_buckets) - 1);
    if (!tryBucketInsert(bucket, key, data))
        return false;

    /// Try to find a place for this item via iterative eviction/relocation.
    return evictRelocateInsert(bucket, argument_key, argument_data);
}

/// jemalloc: ckh_rebuild
bool CuckooHashBase::rebuild(const CuckooHashCell * old_table)
{
    size_t total = count_;
    count_ = 0;
    for (size_t i = 0, num_insert = 0; num_insert < total; ++i)
    {
        if (old_table[i].key != nullptr)
        {
            const void * key = old_table[i].key;
            const void * data = old_table[i].data;
            if (tryInsert(&key, &data))
            {
                count_ = total;
                return true;
            }
            ++num_insert;
        }
    }
    return false;
}

/// The first part of jemalloc's `ckh_new`.
unsigned CuckooHashBase::initFields(size_t min_items, CuckooHashFunction hash_function, CuckooKeyCompare key_compare_function)
{
    ALLOCATOR_ASSERT(min_items > 0);
    ALLOCATOR_ASSERT(hash_function != nullptr);
    ALLOCATOR_ASSERT(key_compare_function != nullptr);

    prng_state = 42; /// Value doesn't really matter.
    count_ = 0;

    /// Find the minimum power of 2 that is large enough to fit minitems entries. We are using (2+,2) cuckoo hashing,
    /// which has an expected maximum load factor of at least ~0.86, so 0.75 is a conservative load factor that will
    /// typically allow mincells items to fit without ever growing the table.
    size_t min_cells = ((min_items + (3 - (min_items % 3))) / 3) << 2;
    unsigned log2_min_cells = LOG2_CUCKOO_HASH_BUCKET_CELLS;
    while ((size_t(1) << log2_min_cells) < min_cells)
        ++log2_min_cells;
    log2_min_buckets = log2_min_cells - LOG2_CUCKOO_HASH_BUCKET_CELLS;
    log2_current_buckets = log2_min_cells - LOG2_CUCKOO_HASH_BUCKET_CELLS;
    hash = hash_function;
    key_compare = key_compare_function;
    table = nullptr;
    return log2_min_cells;
}

/// jemalloc: ckh_iter
bool CuckooHashBase::iterate(size_t * table_idx, void ** key, void ** data) const
{
    for (size_t i = *table_idx, num_cells = numCells(); i < num_cells; ++i)
    {
        if (table[i].key != nullptr)
        {
            if (key != nullptr)
                *key = const_cast<void *>(table[i].key);
            if (data != nullptr)
                *data = const_cast<void *>(table[i].data);
            *table_idx = i + 1;
            return false;
        }
    }
    return true;
}

/// The first part of jemalloc's `ckh_remove`.
bool CuckooHashBase::removeCell(size_t cell, void ** key, void ** data)
{
    if (key != nullptr)
        *key = const_cast<void *>(table[cell].key);
    if (data != nullptr)
        *data = const_cast<void *>(table[cell].data);
    table[cell].key = nullptr;
    table[cell].data = nullptr; /// Not necessary.

    --count_;
    /// Try to halve the table if it is less than 1/4 full.
    return count_ < (size_t(1) << (log2_current_buckets + LOG2_CUCKOO_HASH_BUCKET_CELLS - 2)) && log2_current_buckets > log2_min_buckets;
}

/// jemalloc: ckh_search
bool CuckooHashBase::search(const void * search_key, void ** key, void ** data) const
{
    size_t cell = searchInternal(search_key);
    if (cell == NOT_FOUND)
        return true;
    if (key != nullptr)
        *key = const_cast<void *>(table[cell].key);
    if (data != nullptr)
        *data = const_cast<void *>(table[cell].data);
    return false;
}

/// jemalloc: ckh_string_hash
void cuckooHashStringHash(const void * key, size_t result_hash[2])
{
    hash::hash(key, std::strlen(static_cast<const char *>(key)), 0x94122f33U, result_hash);
}

/// jemalloc: ckh_string_keycomp
bool cuckooHashStringKeyCompare(const void * k1, const void * k2)
{
    ALLOCATOR_ASSERT(k1 != nullptr);
    ALLOCATOR_ASSERT(k2 != nullptr);
    return std::strcmp(static_cast<const char *>(k1), static_cast<const char *>(k2)) == 0;
}

/// jemalloc: ckh_pointer_hash
void cuckooHashPointerHash(const void * key, size_t result_hash[2])
{
    size_t i = reinterpret_cast<uintptr_t>(key);
    hash::hash(&i, sizeof(i), 0xd983396eU, result_hash);
}

/// jemalloc: ckh_pointer_keycomp
bool cuckooHashPointerKeyCompare(const void * k1, const void * k2)
{
    return k1 == k2;
}

}
