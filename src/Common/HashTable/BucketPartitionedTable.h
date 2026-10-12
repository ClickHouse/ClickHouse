#pragma once

#include <concepts>
#include <base/types.h>
#include <Common/HashTable/PartitionedFixedHashTable.h>

/// A `TwoLevelHashTable` takes the bucket from `hash_value`.
/// A `PartitionedFixedHashTable` takes it from the cache line that the cell of `key` starts on, see `bucketOfKey`.
template <typename Map>
requires is_partitioned_fixed_table<Map> || requires(size_t hash_value) { Map::getBucketFromHash(hash_value); }
size_t getBucketOfKey(const typename Map::key_type & key, size_t hash_value)
{
    if constexpr (is_partitioned_fixed_table<Map>)
        return Map::bucketOfKey(key);
    else
        return Map::getBucketFromHash(hash_value);
}

/// A `PartitionedFixedHashTable` has one buffer, allocated when the table is created. Bucket 0 counts it.
template <typename Map>
size_t getBucketBufferSizeInBytes(const Map & map, size_t bucket)
{
    if constexpr (is_partitioned_fixed_table<Map>)
        return bucket == 0 ? map.getBufferSizeInBytes() : 0;
    else
        return map.impls[bucket].getBufferSizeInBytes();
}

/// True when each bucket has its own buffer, so the buckets can be freed apart. A `PartitionedFixedHashTable`
/// has one buffer for all its buckets, and a table with one bucket has nothing to split.
template <typename Map>
constexpr bool has_buffer_per_bucket = !is_partitioned_fixed_table<Map> && Map::NUM_BUCKETS > 1;

/// Returns false when the table has nothing to reserve: a `PartitionedFixedHashTable` has all its cells from the start.
template <typename Map>
bool reserveBucket(Map & map, size_t bucket, size_t num_elements)
{
    if constexpr (is_partitioned_fixed_table<Map>)
        return false;
    else
    {
        map.impls[bucket].reserve(num_elements);
        return true;
    }
}

/// Call after the last insert of a fill by bucket, before the table is read. A `TwoLevelHashTable` computes
/// the prefix sums that `offsetInternal` needs. A `PartitionedFixedHashTable` restores its min/max bounds.
template <typename Map>
void finishConcurrentFill(Map & map)
{
    map.computeBucketPrefix();
    if constexpr (is_partitioned_fixed_table<Map>)
        map.restoreMinMaxOptimization();
}

/** What a caller that fills a table bucket by bucket relies on, whether the table is a `TwoLevelHashTable`
  * or a `PartitionedFixedHashTable`.
  *
  * The bucket of a key is `getBucketOfKey<Map>(key, map.hash(key))`.
  * An iterator's `getBucket` is the bucket of the key it points to.
  * `offsetInternal` numbers cells across all buckets.
  */
template <typename Map>
concept BucketPartitionedTable = requires(
    Map & map,
    const Map & const_map,
    typename Map::key_type key,
    typename Map::LookupResult & lookup,
    typename Map::ConstLookupResult const_lookup,
    bool & inserted,
    size_t hash_value)
{
    typename Map::key_type;
    typename Map::mapped_type;
    typename Map::value_type;
    typename Map::cell_type;
    typename Map::LookupResult;
    typename Map::ConstLookupResult;
    typename Map::iterator;
    typename Map::const_iterator;

    { const_map.hash(key) } -> std::convertible_to<size_t>;
    { getBucketOfKey<Map>(key, hash_value) } -> std::convertible_to<size_t>;
    { Map::NUM_BUCKETS } -> std::convertible_to<UInt32>;

    map.emplace(key, lookup, inserted);
    map.emplace(key, lookup, inserted, hash_value);
    { map.find(key) } -> std::same_as<typename Map::LookupResult>;
    { map.find(key, hash_value) } -> std::same_as<typename Map::LookupResult>;

    map.computeBucketPrefix();
    { const_map.offsetInternal(const_lookup) } -> std::convertible_to<size_t>;

    { const_map.size() } -> std::convertible_to<size_t>;
    { const_map.empty() } -> std::same_as<bool>;
    { const_map.getBufferSizeInBytes() } -> std::convertible_to<size_t>;
    { const_map.getBufferSizeInCells() } -> std::convertible_to<size_t>;

    { map.begin() } -> std::same_as<typename Map::iterator>;
    { map.end() } -> std::same_as<typename Map::iterator>;
    { const_map.begin() } -> std::same_as<typename Map::const_iterator>;
    { const_map.end() } -> std::same_as<typename Map::const_iterator>;
    { map.begin().getBucket() } -> std::convertible_to<size_t>;
    { const_map.begin().getBucket() } -> std::convertible_to<size_t>;
};

template <typename Map>
concept BucketPartitionedMap = BucketPartitionedTable<Map> && requires(Map & map)
{
    map.forEachMapped([](typename Map::mapped_type &) {});
};
