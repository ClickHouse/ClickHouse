#include <gtest/gtest.h>

#include <Common/CacheLine.h>
#include <Common/HashTable/PartitionedFixedHashMap.h>
#include <Common/HashTable/PartitionedFixedHashSet.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>


/** The invariant under test: partitioning a fixed-range table changes which bucket a key belongs to
  * and nothing else. Cells, offsets, buffer size, iteration and serialization must be those of the
  * plain `FixedHashMap` at every bucket count.
  */

namespace
{

template <typename Key, size_t size_bits, size_t BITS_FOR_BUCKET>
using Partitioned = PartitionedFixedHashMap<Key, UInt64, size_bits, BITS_FOR_BUCKET>;

template <typename Key, size_t size_bits>
using Plain = FixedHashMapWithSizeBits<Key, UInt64, size_bits>;

template <typename Map>
void insertKeyValue(Map & map, typename Map::key_type key, UInt64 value)
{
    typename Map::LookupResult it = nullptr;
    bool inserted = false;
    map.emplace(key, it, inserted);
    if (inserted)
        new (&it->getMapped()) UInt64(value);
    else
        it->getMapped() = value;
}

/// The bucket a key belongs to, computed the way an external caller would before taking the bucket's lock.
template <typename Map>
size_t routedBucket(typename Map::key_type key)
{
    return Map::getBucketFromHash(Map::bucketRoutingHash(key, Map::hash(key)));
}

template <typename Map>
std::vector<size_t> offsetsByIteration(const Map & map)
{
    std::vector<size_t> offsets;
    for (typename Map::const_iterator it = map.begin(); it != map.end(); ++it)
        offsets.push_back(map.offsetInternal(it.getPtr()));
    return offsets;
}

template <size_t... bits, typename Fn>
void forBucketBits(Fn && fn)
{
    (fn.template operator()<bits>(), ...);
}

template <typename Map>
std::string serialize(const Map & map)
{
    DB::WriteBufferFromOwnString wb;
    map.write(wb);
    return wb.str();
}

template <typename Map>
void deserialize(Map & map, const std::string & bytes)
{
    DB::ReadBufferFromString rb(bytes);
    map.read(rb);
    EXPECT_TRUE(rb.eof()) << "write emitted more than read consumed";
}

/// Keys are pairwise consecutive; the two must route together whenever their cells start on one cache line.
template <typename Map>
void assertCacheLinesNeverSpanBuckets(size_t num_keys)
{
    constexpr size_t cell_size = sizeof(typename Map::cell_type);
    for (size_t key = 1; key < num_keys; ++key)
    {
        if (((key - 1) * cell_size) / DB::CH_CACHE_LINE_SIZE != (key * cell_size) / DB::CH_CACHE_LINE_SIZE)
            continue;
        const auto typed_key = static_cast<typename Map::key_type>(key);
        const auto typed_prev = static_cast<typename Map::key_type>(key - 1);
        ASSERT_EQ(routedBucket<Map>(typed_key), routedBucket<Map>(typed_prev))
            << "keys " << (key - 1) << " and " << key << " share a cache line but route apart";
    }
}

/// At least `min_buckets` of the buckets receive keys and no bucket takes more than a quarter of them.
template <typename Map>
void assertKeysSpread(const std::vector<typename Map::key_type> & keys, size_t min_buckets)
{
    std::vector<size_t> per_bucket(Map::NUM_BUCKETS, 0);
    for (const auto key : keys)
        ++per_bucket[routedBucket<Map>(key)];

    const size_t non_empty = std::count_if(per_bucket.begin(), per_bucket.end(), [](size_t count) { return count != 0; });
    const size_t largest = *std::max_element(per_bucket.begin(), per_bucket.end());
    ASSERT_GE(non_empty, min_buckets) << "keys reached only " << non_empty << " of " << Map::NUM_BUCKETS << " buckets";
    ASSERT_LE(largest, keys.size() / 4) << "one bucket took " << largest << " of " << keys.size() << " keys";
}

}


TEST(PartitionedFixedHashMap, CellsAndOffsetsMatchThePlainMap)
{
    constexpr size_t size_bits = 16;
    constexpr UInt32 num_keys = 5000;

    Plain<UInt32, size_bits> plain;
    for (UInt32 key = 0; key < num_keys; ++key)
        insertKeyValue(plain, key, key);

    forBucketBits<0, 8>(
        [&]<size_t bits>()
        {
            using Map = Partitioned<UInt32, size_bits, bits>;
            Map map;
            for (UInt32 key = 0; key < num_keys; ++key)
                insertKeyValue(map, key, key);

            ASSERT_EQ(Map::NUM_BUCKETS, 1u << bits);
            ASSERT_EQ(map.size(), num_keys) << "bits " << bits;
            for (UInt32 key = 0; key < num_keys; ++key)
            {
                const auto * cell = map.find(key);
                ASSERT_NE(cell, nullptr) << "key " << key << ", bits " << bits;
                ASSERT_EQ(map.offsetInternal(cell), plain.offsetInternal(plain.find(key))) << "key " << key << ", bits " << bits;
                ASSERT_TRUE(map.has(key)) << "key " << key << ", bits " << bits;
            }
            ASSERT_EQ(map.find(num_keys + 1), nullptr) << "bits " << bits;
        });
}


TEST(PartitionedFixedHashMap, BufferSizeIsIndependentOfBucketCount)
{
    constexpr size_t size_bits = 16;
    constexpr size_t expected_cells = 1ULL << size_bits;

    forBucketBits<0, 1, 4, 8>(
        [&]<size_t bits>()
        {
            using Map = Partitioned<UInt32, size_bits, bits>;
            Map map;
            ASSERT_EQ(Map::NUM_BUCKETS, 1u << bits);
            ASSERT_TRUE(map.empty());
            ASSERT_EQ(map.getBufferSizeInCells(), expected_cells) << "bits " << bits;
            ASSERT_EQ(map.getBufferSizeInBytes(), expected_cells * sizeof(typename Map::cell_type)) << "bits " << bits;
            for (UInt32 i = 1; i < Map::NUM_BUCKETS; ++i)
                ASSERT_EQ(map.impls[i].getBufferSizeInBytes(), map.impls[0].getBufferSizeInBytes()) << "bits " << bits;
        });
}


TEST(PartitionedFixedHashMap, IterationVisitsEveryCellOnce)
{
    /// The buckets share the cells, so a walk over buckets would visit the table once per bucket.
    constexpr size_t size_bits = 16;
    constexpr UInt32 num_keys = 3000;

    forBucketBits<0, 8>(
        [&]<size_t bits>()
        {
            Partitioned<UInt32, size_bits, bits> map;
            for (UInt32 key = 0; key < num_keys; ++key)
                insertKeyValue(map, key, key * 3);

            const auto offsets = offsetsByIteration(map);
            ASSERT_EQ(offsets.size(), num_keys) << "bits " << bits;
            const std::unordered_set<size_t> unique(offsets.begin(), offsets.end());
            ASSERT_EQ(unique.size(), num_keys) << "a cell was visited twice at bits " << bits;
            for (UInt32 key = 0; key < num_keys; ++key)
                ASSERT_TRUE(unique.contains(map.offsetInternal(map.find(key)))) << "key " << key << " was not visited at bits " << bits;

            size_t visited = 0;
            map.forEachMapped(
                [&](UInt64 & mapped)
                {
                    ++visited;
                    mapped += 1;
                });
            ASSERT_EQ(visited, num_keys) << "bits " << bits;
            for (UInt32 key = 0; key < num_keys; ++key)
                ASSERT_EQ(map.find(key)->getMapped(), key * 3 + 1) << "key " << key;
        });
}


TEST(PartitionedFixedHashMap, RoutingIsInRangeAndStable)
{
    constexpr size_t size_bits = 16;
    constexpr UInt32 num_keys = 4000;

    forBucketBits<0, 8>(
        [&]<size_t bits>()
        {
            using Map = Partitioned<UInt32, size_bits, bits>;
            Map map;

            std::vector<size_t> bucket_of_key(num_keys);
            for (UInt32 key = 0; key < num_keys; ++key)
            {
                bucket_of_key[key] = routedBucket<Map>(key);
                ASSERT_LT(bucket_of_key[key], Map::NUM_BUCKETS) << "key " << key;
            }

            /// A key read under a different lock than it was written under would be a data race.
            for (UInt32 key = 0; key < num_keys; ++key)
            {
                insertKeyValue(map, key, key);
                ASSERT_EQ(routedBucket<Map>(key), bucket_of_key[key]) << "routing moved for key " << key;
            }
        });
}


TEST(PartitionedFixedHashMap, ACacheLineNeverSpansTwoBuckets)
{
    /// A `bool` plus an 8-byte, 4-aligned payload is 12 bytes. The cell pads that to 16, which divides the line.
    assertCacheLinesNeverSpanBuckets<Partitioned<UInt32, 16, 8>>(1u << 16);

    struct alignas(4) TwoWords
    {
        UInt32 a = 0;
        UInt32 b = 0;
    };
    using PaddedCellMap = PartitionedFixedHashMap<UInt8, TwoWords, 8, 8>;
    static_assert(sizeof(PaddedCellMap::cell_type) == 16);
    static_assert(DB::CH_CACHE_LINE_SIZE % sizeof(PaddedCellMap::cell_type) == 0);
    assertCacheLinesNeverSpanBuckets<PaddedCellMap>(256);
}


TEST(PartitionedFixedHashMap, SpreadsKeysThatShareHighOrLowBits)
{
    constexpr size_t size_bits = 16;
    using Map = Partitioned<UInt32, size_bits, 4>;
    ASSERT_EQ(Map::NUM_BUCKETS, 16u);

    /// 300 keys at the bottom of a 65536-cell table share their high bits; routing on those would
    /// put all of them into one bucket.
    std::vector<UInt32> dense_at_zero(300);
    for (UInt32 key = 0; key < dense_at_zero.size(); ++key)
        dense_at_zero[key] = key;
    assertKeysSpread<Map>(dense_at_zero, 14);

    /// Multiples of 256 share their low bits.
    std::vector<UInt32> strided(256);
    for (UInt32 i = 0; i < strided.size(); ++i)
        strided[i] = i * 256;
    assertKeysSpread<Map>(strided, 14);
}


TEST(PartitionedFixedHashMap, SmallKeyTypeIsFullyAddressable)
{
    /// Every key of `UInt8` must be reachable, also with more buckets than the table has cache lines.
    forBucketBits<0, 8>(
        [&]<size_t bits>()
        {
            Partitioned<UInt8, 8, bits> map;
            for (size_t key = 0; key < 256; ++key)
                insertKeyValue(map, static_cast<UInt8>(key), key);

            ASSERT_EQ(map.size(), 256u) << "bits " << bits;
            ASSERT_EQ(map.getBufferSizeInCells(), 256u) << "bits " << bits;
            for (size_t key = 0; key < 256; ++key)
            {
                const auto * cell = map.find(static_cast<UInt8>(key));
                ASSERT_NE(cell, nullptr) << "key " << key << ", bits " << bits;
                ASSERT_EQ(cell->getMapped(), key);
                ASSERT_EQ(map.offsetInternal(cell), key + 1) << "key " << key;
            }
            ASSERT_EQ(offsetsByIteration(map).size(), 256u) << "bits " << bits;
        });
}


TEST(PartitionedFixedHashMap, MinMaxOptimizationIsOffOnlyWhileBucketsAreFilled)
{
    /// One bucket has one writer, so the bounds stay live. Several buckets have several writers, who
    /// would race on the bounds; they are off until the caller restores them after the build.
    Partitioned<UInt16, 16, 0> serial;
    Partitioned<UInt16, 16, 8> parallel;
    for (const UInt16 key : std::initializer_list<UInt16>{10, 20, 40})
    {
        insertKeyValue(serial, key, key);
        insertKeyValue(parallel, key, key);
    }

    ASSERT_TRUE(serial.canUseMinMaxOptimization());
    ASSERT_EQ(offsetsByIteration(serial).size(), 3u);

    ASSERT_FALSE(parallel.canUseMinMaxOptimization());
    ASSERT_EQ(offsetsByIteration(parallel).size(), 3u);
    parallel.restoreMinMaxOptimization();
    ASSERT_TRUE(parallel.canUseMinMaxOptimization());
    ASSERT_EQ(offsetsByIteration(parallel).size(), 3u);
}


TEST(PartitionedFixedHashMap, ConcurrentInsertsUnderOneLockPerBucket)
{
    constexpr size_t num_threads = 16;
    constexpr UInt32 keys_per_thread = 10000;
    using Map = Partitioned<UInt32, 18, 6>;
    ASSERT_EQ(Map::NUM_BUCKETS, 64u);

    /// Distinct keys are distinct cells, so keys under different locks never touch the same cell.
    /// The same keys collide inside their bucket. Both must end in the same state.
    for (const bool same_keys_in_every_thread : {false, true})
    {
        Map map;
        std::vector<std::mutex> bucket_mutexes(Map::NUM_BUCKETS);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (size_t t = 0; t < num_threads; ++t)
        {
            const UInt32 first_key = same_keys_in_every_thread ? 0 : static_cast<UInt32>(t) * keys_per_thread;
            threads.emplace_back([&map, &bucket_mutexes, first_key]
            {
                for (UInt32 key = first_key; key < first_key + keys_per_thread; ++key)
                {
                    std::lock_guard lock(bucket_mutexes[routedBucket<Map>(key)]);
                    insertKeyValue(map, key, key * 5);
                }
            });
        }
        for (auto & thread : threads)
            thread.join();

        const UInt32 total_keys = same_keys_in_every_thread ? keys_per_thread : num_threads * keys_per_thread;
        ASSERT_EQ(map.size(), total_keys);
        for (UInt32 key = 0; key < total_keys; ++key)
        {
            const auto * cell = map.find(key);
            ASSERT_NE(cell, nullptr) << "key " << key << " was lost";
            ASSERT_EQ(cell->getMapped(), key * 5) << "key " << key;
            ASSERT_EQ(map.offsetInternal(cell), key + 1) << "key " << key;
        }
        ASSERT_EQ(offsetsByIteration(map).size(), total_keys);
    }
}


TEST(PartitionedFixedHashMap, IteratorReportsTheRoutedBucket)
{
    /// The storage is flat, so `getBucket` is 0 for every cell; a scan split by bucket uses `getRoutedBucket`.
    using Map = Partitioned<UInt16, 16, 8>;
    Map map;
    constexpr UInt16 num_keys = 4000;
    for (UInt16 key = 0; key < num_keys; ++key)
        insertKeyValue(map, key, key);

    constexpr size_t num_streams = 4;
    std::vector<size_t> per_stream(num_streams, 0);
    std::vector<char> seen(num_keys, 0);
    for (auto it = map.begin(); it != map.end(); ++it)
    {
        ASSERT_EQ(it.getBucket(), 0u);
        const auto key = static_cast<UInt16>(it.getHash());
        ASSERT_LT(key, num_keys);
        ASSERT_EQ(it.getRoutedBucket(), routedBucket<Map>(key)) << "key " << key;
        ASSERT_FALSE(seen[key]) << "key " << key << " visited twice";
        seen[key] = 1;
        ++per_stream[it.getRoutedBucket() % num_streams];
    }
    ASSERT_TRUE(std::all_of(seen.begin(), seen.end(), [](char c) { return c == 1; }));
    for (size_t stream = 0; stream < num_streams; ++stream)
        ASSERT_GT(per_stream[stream], 0u) << "stream " << stream << " would get no cells";
}


TEST(PartitionedFixedHashSet, RecordsPresenceAndRoutesByCacheLine)
{
    /// A set cell is one byte, so a cache line holds many keys, and all of them route to one bucket.
    /// Keys one line apart are what spread over the buckets.
    constexpr size_t keys_per_line = DB::CH_CACHE_LINE_SIZE / sizeof(FixedHashTableCell<UInt16>);
    constexpr UInt32 num_lines = 1000;

    forBucketBits<0, 8>(
        [&]<size_t bits>()
        {
            using Set = PartitionedFixedHashSet<UInt16, 16, bits>;
            Set set;
            ASSERT_EQ(Set::NUM_BUCKETS, 1u << bits);
            ASSERT_EQ(set.getBufferSizeInCells(), 1u << 16) << "bits " << bits;

            std::unordered_set<size_t> buckets;
            for (UInt32 line = 0; line < num_lines; ++line)
            {
                const auto key = static_cast<UInt16>(line * keys_per_line);
                typename Set::LookupResult it = nullptr;
                bool inserted = false;
                set.emplace(key, it, inserted);
                ASSERT_TRUE(inserted) << "key " << key;
                buckets.insert(routedBucket<Set>(key));
            }
            ASSERT_EQ(set.size(), num_lines) << "bits " << bits;
            if (bits == 0)
                ASSERT_EQ(buckets.size(), 1u);
            else
                ASSERT_GT(buckets.size(), 200u) << "keys on distinct cache lines reached only " << buckets.size() << " buckets";

            for (UInt16 key = 0; key < keys_per_line; ++key)
                ASSERT_EQ(routedBucket<Set>(key), routedBucket<Set>(0))
                    << "key " << key << " shares a cache line with key 0 but routes apart";

            for (UInt32 line = 0; line < num_lines; ++line)
                ASSERT_TRUE(set.has(static_cast<UInt16>(line * keys_per_line))) << "line " << line;
            ASSERT_FALSE(set.has(static_cast<UInt16>(1)));

            size_t iterated = 0;
            for (auto it = set.begin(); it != set.end(); ++it)
                ++iterated;
            ASSERT_EQ(iterated, num_lines) << "bits " << bits;

            /// Presence is all a set cell carries, so the bytes are the table's populated positions.
            Set copy;
            deserialize(copy, serialize(set));
            ASSERT_EQ(copy.size(), num_lines) << "bits " << bits;
            for (UInt32 line = 0; line < num_lines; ++line)
                ASSERT_TRUE(copy.has(static_cast<UInt16>(line * keys_per_line))) << "line " << line;
        });
}
