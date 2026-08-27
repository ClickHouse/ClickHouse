#include <gtest/gtest.h>

#include <Common/CacheLine.h>
#include <Common/HashTable/PartitionedFixedHashMap.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>


/** The invariant under test throughout: routing changes which lock a key belongs under and nothing
  * else. Cells, offsets, buffer size and iteration must be indistinguishable from a plain
  * `FixedHashMap` at every bucket count.
  */

namespace
{

using Mapped = UInt64;

template <typename Key, size_t size_bits, Int32 bits_for_bucket>
using Partitioned = PartitionedFixedHashMap<Key, Mapped, size_bits, bits_for_bucket>;

template <typename Key, size_t size_bits>
using Plain = FixedHashMapWithSizeBits<Key, Mapped, size_bits>;

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

/// The pair a caller must use: for a direct-addressed table the cell hash cannot select a bucket.
template <typename Map>
size_t routedBucket(const Map & map, typename Map::key_type key)
{
    return map.getBucketFromHash(map.bucketRoutingHash(key, map.hash(key)));
}

template <typename Map>
constexpr size_t cellSize()
{
    return sizeof(typename Map::cell_type);
}

/// Collected the way `NotJoinedHash::fillColumns` does, through the iterator's raw cell pointer.
template <typename Map>
std::vector<size_t> offsetsByIteration(Map & map)
{
    std::vector<size_t> offsets;
    for (auto it = map.begin(); it != map.end(); ++it)
        offsets.push_back(map.offsetInternal(it.getPtr()));
    return offsets;
}

template <typename Fn>
void forSerialAndParallelBits(Fn && fn)
{
    fn.template operator()<0>();
    fn.template operator()<8>();
}

template <typename Map>
std::string serializeMap(const Map & map)
{
    DB::WriteBufferFromOwnString wb;
    map.write(wb);
    return wb.str();
}

template <typename Map>
Map roundTripMap(const Map & src)
{
    const auto bytes = serializeMap(src);
    Map dst;
    DB::ReadBufferFromString rb(bytes);
    dst.read(rb);
    EXPECT_TRUE(rb.eof()) << "serialized more than serializedPartitionCount() partitions";
    return dst;
}
}


TEST(PartitionedFixedHashMap, OffsetsMatchAPlainFixedHashMap)
{
    /// `HashJoin` sizes its used flags from the cell count and indexes them by `offsetInternal`.
    constexpr size_t size_bits = 16;
    constexpr UInt32 num_keys = 5000;

    Plain<UInt32, size_bits> plain;
    for (UInt32 key = 0; key < num_keys; ++key)
        insertKeyValue(plain, key, key);

    forSerialAndParallelBits(
        [&]<Int32 bits>()
        {
            Partitioned<UInt32, size_bits, bits> map;
            for (UInt32 key = 0; key < num_keys; ++key)
                insertKeyValue(map, key, key);

            ASSERT_EQ(map.size(), num_keys) << "bits " << bits;
            ASSERT_EQ(decltype(map)::numBuckets(), 1u << bits) << "bits " << bits;

            for (UInt32 key = 0; key < num_keys; ++key)
            {
                const auto * partitioned_cell = map.find(key);
                const auto * plain_cell = plain.find(key);
                ASSERT_NE(partitioned_cell, nullptr) << "key " << key << ", bits " << bits;
                ASSERT_NE(plain_cell, nullptr) << "key " << key;
                ASSERT_EQ(map.offsetInternal(partitioned_cell), plain.offsetInternal(plain_cell))
                    << "key " << key << " got a different offset at bits " << bits;
                ASSERT_TRUE(map.has(key)) << "key " << key << ", bits " << bits;
            }

            ASSERT_EQ(map.find(num_keys + 1), nullptr) << "bits " << bits;
        });
}


TEST(PartitionedFixedHashMap, BufferSizeIsIndependentOfBucketCount)
{
    /// If this fails the used-flags array grows with the bucket count and one flat buffer buys
    /// nothing.
    constexpr size_t size_bits = 16;
    constexpr size_t expected_cells = 1ULL << size_bits;

    auto check = [&]<Int32 bits>()
    {
        Partitioned<UInt32, size_bits, bits> map;
        ASSERT_EQ(decltype(map)::numBuckets(), 1u << bits);
        ASSERT_EQ(map.getBufferSizeInCells(), expected_cells) << "bits " << bits;
        ASSERT_EQ(map.getBufferSizeInBytes(), expected_cells * sizeof(typename Partitioned<UInt32, size_bits, bits>::cell_type))
            << "bits " << bits;
        ASSERT_TRUE(map.empty());
    };
    check.template operator()<0>();
    check.template operator()<1>();
    check.template operator()<4>();
    check.template operator()<8>();
}


TEST(PartitionedFixedHashMap, IterationVisitsEveryCellExactlyOnce)
{
    /// Buckets share the cells, so a per-bucket walk would traverse the table once per bucket.
    constexpr size_t size_bits = 16;
    constexpr UInt32 num_keys = 3000;

    forSerialAndParallelBits(
        [&]<Int32 bits>()
        {
            Partitioned<UInt32, size_bits, bits> map;
            for (UInt32 key = 0; key < num_keys; ++key)
                insertKeyValue(map, key, key * 3);

            const auto offsets = offsetsByIteration(map);
            ASSERT_EQ(offsets.size(), num_keys) << "bits " << bits;

            const std::unordered_set<size_t> unique(offsets.begin(), offsets.end());
            ASSERT_EQ(unique.size(), num_keys) << "iteration repeated a cell at bits " << bits;

            for (UInt32 key = 0; key < num_keys; ++key)
                ASSERT_TRUE(unique.contains(map.offsetInternal(map.find(key)))) << "key " << key << " was never visited at bits " << bits;

            /// `tryRerangeRightTableData` rewrites mapped values through this.
            size_t visited = 0;
            map.forEachMapped(
                [&](UInt64 & mapped)
                {
                    ++visited;
                    mapped += 1;
                });
            ASSERT_EQ(visited, num_keys) << "forEachMapped at bits " << bits;
            for (UInt32 key = 0; key < num_keys; ++key)
                ASSERT_EQ(map.find(key)->getMapped(), key * 3 + 1) << "key " << key;
        });
}


TEST(PartitionedFixedHashMap, EveryKeyRoutesToExactlyOneBucketAndRoutingIsStable)
{
    constexpr size_t size_bits = 16;
    constexpr UInt32 num_keys = 4000;

    forSerialAndParallelBits(
        [&]<Int32 bits>()
        {
            Partitioned<UInt32, size_bits, bits> map;
            const size_t num_buckets = decltype(map)::numBuckets();

            std::vector<size_t> bucket_of_key(num_keys);
            for (UInt32 key = 0; key < num_keys; ++key)
            {
                bucket_of_key[key] = routedBucket(map, key);
                ASSERT_LT(bucket_of_key[key], num_buckets) << "key " << key;
            }

            /// If build and probe routing could drift, a row would be read under the wrong lock.
            for (UInt32 key = 0; key < num_keys; ++key)
            {
                insertKeyValue(map, key, key);
                ASSERT_EQ(routedBucket(map, key), bucket_of_key[key]) << "routing moved for key " << key;
            }
        });
}


TEST(PartitionedFixedHashMap, ACacheLineNeverSpansTwoBuckets)
{
    /// Keys whose cells start on the same cache line must share a bucket. `sizeof(Cell)` for
    /// `Mapped = UInt64` divides the line, so this is the aligned case.
    constexpr size_t size_bits = 16;
    using Map = Partitioned<UInt32, size_bits, 8>;
    Map map;
    constexpr size_t cell_size = cellSize<Map>();

    for (UInt32 key = 1; key < (1U << size_bits); ++key)
    {
        if (((key - 1) * cell_size) / DB::CH_CACHE_LINE_SIZE != (key * cell_size) / DB::CH_CACHE_LINE_SIZE)
            continue;
        ASSERT_EQ(routedBucket(map, key), routedBucket(map, key - 1))
            << "keys " << (key - 1) << " and " << key << " start on one cache line but route apart";
    }
}


TEST(PartitionedFixedHashMap, MapsOneSizedCellsKeepAStartedCacheLineUnderOneLock)
{
    /// `HashJoin::MapsOne` stores `RowRef` (8 bytes, align 4) in `FixedHashMapCell`, so the cell is
    /// 12 bytes and `64 / 12` is not a power of two. Keys 3 and 4 start at offsets 36 and 48.
    struct alignas(4) MapsOneMapped
    {
        UInt32 a = 0;
        UInt32 b = 0;
    };
    using Cell = FixedHashMapCell<UInt8, MapsOneMapped>;
    static_assert(sizeof(Cell) == 12);
    using Map = PartitionedFixedHashMap<UInt8, MapsOneMapped, 8, 8>;
    Map map;
    ASSERT_EQ(sizeof(typename Map::cell_type), 12u);

    ASSERT_EQ(routedBucket(map, static_cast<UInt8>(3)), routedBucket(map, static_cast<UInt8>(4)));

    for (UInt16 key = 1; key < 256; ++key)
    {
        const size_t prev_line = ((key - 1) * 12) / DB::CH_CACHE_LINE_SIZE;
        const size_t line = (key * 12) / DB::CH_CACHE_LINE_SIZE;
        if (prev_line != line)
            continue;
        ASSERT_EQ(routedBucket(map, static_cast<UInt8>(key)), routedBucket(map, static_cast<UInt8>(key - 1)))
            << "keys " << (key - 1) << " and " << key;
    }
}


TEST(PartitionedFixedHashMap, SpreadsDenseAtZeroKeys)
{
    /// Rules out high-bit routing: a 300-key range lives in a 65536-cell table, where every key
    /// shares its high bits, so all 300 would land in bucket 0.
    constexpr size_t size_bits = 16;
    constexpr UInt32 num_keys = 300;
    using Map = Partitioned<UInt32, size_bits, 4>;
    Map map;
    const size_t num_buckets = decltype(map)::numBuckets();
    ASSERT_EQ(num_buckets, 16u);

    std::vector<size_t> per_bucket(num_buckets, 0);
    for (UInt32 key = 0; key < num_keys; ++key)
    {
        insertKeyValue(map, key, key);
        ++per_bucket[routedBucket(map, key)];
    }

    size_t non_empty = 0;
    size_t largest = 0;
    for (const size_t count : per_bucket)
    {
        non_empty += count != 0;
        largest = std::max(largest, count);
    }

    ASSERT_GE(non_empty, 14u) << "dense-at-zero keys reached only " << non_empty << " of " << num_buckets << " buckets";
    ASSERT_LE(largest, num_keys / 4) << "one bucket took " << largest << " of " << num_keys << " keys";
}


TEST(PartitionedFixedHashMap, SpreadsKeysWithLowBitStructure)
{
    /// Rules out low-bit routing: keys that are all multiples of 256 share their low bits.
    constexpr size_t size_bits = 16;
    constexpr UInt32 stride = 256;
    constexpr UInt32 num_keys = 256;
    using Map = Partitioned<UInt32, size_bits, 4>;
    Map map;
    const size_t num_buckets = decltype(map)::numBuckets();
    ASSERT_EQ(num_buckets, 16u);

    std::vector<size_t> per_bucket(num_buckets, 0);
    for (UInt32 i = 0; i < num_keys; ++i)
    {
        const UInt32 key = i * stride;
        insertKeyValue(map, key, key);
        ++per_bucket[routedBucket(map, key)];
    }

    size_t non_empty = 0;
    size_t largest = 0;
    for (const size_t count : per_bucket)
    {
        non_empty += count != 0;
        largest = std::max(largest, count);
    }

    ASSERT_GE(non_empty, 14u) << "aligned keys reached only " << non_empty << " of " << num_buckets << " buckets";
    ASSERT_LE(largest, num_keys / 4) << "one bucket took " << largest << " of " << num_keys << " keys";
}


TEST(PartitionedFixedHashMap, SmallKeyTypeIsFullyAddressable)
{
    /// `key8` covers its whole key space, including at more buckets than there are cache lines.
    constexpr size_t size_bits = 8;

    forSerialAndParallelBits(
        [&]<Int32 bits>()
        {
            Partitioned<UInt8, size_bits, bits> map;
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
                ASSERT_TRUE(map.has(static_cast<UInt8>(key)));
            }

            ASSERT_EQ(offsetsByIteration(map).size(), 256u) << "bits " << bits;
        });
}


TEST(PartitionedFixedHashMap, SerialLayoutKeepsMinMaxOptimization)
{
    using Map = Partitioned<UInt16, 16, 0>;
    Map map;
    insertKeyValue(map, 10, 1);
    insertKeyValue(map, 20, 2);
    insertKeyValue(map, 40, 3);

    ASSERT_EQ(decltype(map)::numBuckets(), 1u);
    ASSERT_TRUE(map.canUseMinMaxOptimization());
    ASSERT_EQ(offsetsByIteration(map).size(), 3u);
}


TEST(PartitionedFixedHashMap, ParallelLayoutRestoresMinMaxAfterBuild)
{
    using Map = Partitioned<UInt16, 16, 8>;
    Map map;
    insertKeyValue(map, 10, 1);
    insertKeyValue(map, 20, 2);
    insertKeyValue(map, 40, 3);

    ASSERT_EQ(decltype(map)::numBuckets(), 256u);
    ASSERT_FALSE(map.canUseMinMaxOptimization());
    ASSERT_EQ(offsetsByIteration(map).size(), 3u);

    map.restoreMinMaxOptimization();
    ASSERT_TRUE(map.canUseMinMaxOptimization());
    ASSERT_EQ(offsetsByIteration(map).size(), 3u);
}


TEST(PartitionedFixedHashMap, WriteReadsTheFlatTableOnce)
{
    /// `FixedRangeStorage` must serialize the one buffer, not once per routing bucket.
    using Serial = Partitioned<UInt16, 16, 0>;
    using Parallel = Partitioned<UInt16, 16, 8>;
    ASSERT_EQ(Serial::serializedPartitionCount(), 1u);
    ASSERT_EQ(Parallel::serializedPartitionCount(), 1u);
    ASSERT_EQ(Serial::numBuckets(), 1u);
    ASSERT_EQ(Parallel::numBuckets(), 256u);

    Serial serial;
    Parallel parallel;
    const std::vector<UInt16> keys = {0, 10, 20, 40, 65535};
    for (auto key : keys)
    {
        insertKeyValue(serial, key, key * 3);
        insertKeyValue(parallel, key, key * 3);
    }

    const auto serial_bytes = serializeMap(serial);
    const auto parallel_bytes = serializeMap(parallel);
    ASSERT_EQ(serial_bytes, parallel_bytes);

    auto check_copy = [&](auto & src, auto & copy)
    {
        ASSERT_EQ(copy.size(), src.size());
        ASSERT_EQ(offsetsByIteration(copy), offsetsByIteration(src));
        for (auto key : keys)
        {
            const auto * from = src.find(key);
            const auto * to = copy.find(key);
            ASSERT_NE(from, nullptr) << "key " << key;
            ASSERT_NE(to, nullptr) << "key " << key;
            ASSERT_EQ(to->getMapped(), from->getMapped()) << "key " << key;
            ASSERT_EQ(copy.offsetInternal(to), src.offsetInternal(from)) << "key " << key;
        }
    };

    auto serial_copy = roundTripMap(serial);
    auto parallel_copy = roundTripMap(parallel);
    check_copy(serial, serial_copy);
    check_copy(parallel, parallel_copy);
    check_copy(serial_copy, parallel_copy);
}


TEST(PartitionedFixedHashMap, ConcurrentBuildWithExternalBucketLocks)
{
    /// Distinct keys are distinct cells, so routed disjointness is real disjointness. Nothing
    /// synchronizes internally.
    constexpr size_t size_bits = 18;
    constexpr size_t num_threads = 16;
    constexpr UInt32 keys_per_thread = 10000;
    using Map = Partitioned<UInt32, size_bits, 6>;
    Map map;
    const size_t num_buckets = decltype(map)::numBuckets();
    ASSERT_EQ(num_buckets, 64u);
    std::vector<std::mutex> bucket_mutexes(num_buckets);

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&map, &bucket_mutexes, t]
        {
            const UInt32 begin = static_cast<UInt32>(t) * keys_per_thread;
            for (UInt32 key = begin; key < begin + keys_per_thread; ++key)
            {
                std::lock_guard lock(bucket_mutexes[routedBucket(map, key)]);
                insertKeyValue(map, key, key * 5);
            }
        });
    }
    for (auto & thread : threads)
        thread.join();

    constexpr UInt32 total_keys = num_threads * keys_per_thread;
    ASSERT_EQ(map.size(), total_keys);
    for (UInt32 key = 0; key < total_keys; ++key)
    {
        const auto * cell = map.find(key);
        ASSERT_NE(cell, nullptr) << "key " << key << " lost by the concurrent build";
        ASSERT_EQ(cell->getMapped(), key * 5) << "mapped value of key " << key << " was corrupted";
        ASSERT_EQ(map.offsetInternal(cell), key + 1) << "key " << key;
    }

    ASSERT_EQ(offsetsByIteration(map).size(), total_keys);
}


TEST(PartitionedFixedHashMap, ConcurrentBuildWithContendedKeys)
{
    /// Same, but every thread inserts the same keys, so they collide inside a bucket too.
    constexpr size_t size_bits = 16;
    constexpr size_t num_threads = 16;
    constexpr UInt32 num_keys = 4000;
    using Map = Partitioned<UInt32, size_bits, 5>;
    Map map;
    const size_t num_buckets = decltype(map)::numBuckets();
    ASSERT_EQ(num_buckets, 32u);
    std::vector<std::mutex> bucket_mutexes(num_buckets);

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&map, &bucket_mutexes]
        {
            for (UInt32 key = 0; key < num_keys; ++key)
            {
                std::lock_guard lock(bucket_mutexes[routedBucket(map, key)]);
                insertKeyValue(map, key, key * 11);
            }
        });
    }
    for (auto & thread : threads)
        thread.join();

    ASSERT_EQ(map.size(), num_keys);
    for (UInt32 key = 0; key < num_keys; ++key)
    {
        const auto * cell = map.find(key);
        ASSERT_NE(cell, nullptr) << "key " << key;
        ASSERT_EQ(cell->getMapped(), key * 11) << "key " << key;
    }
}
