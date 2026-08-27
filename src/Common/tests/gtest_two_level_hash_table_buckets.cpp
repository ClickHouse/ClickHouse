#include <gtest/gtest.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/PartitionedFixedHashMap.h>
#include <Common/HashTable/TwoLevelHashTable.h>

#include <bit>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>


/** Tests for the parts of `TwoLevelHashTable` a bucket-parallel JOIN build relies on: the one-bucket
  * bucket count (`bits_for_bucket == 0`, where routing folds away and the table must behave as a
  * single-level one), a bucket-selection hash (`BucketHash`) that may differ from the cell-placement
  * hash, the prefix sums `offsetInternal` numbers cells by, and the direct-addressed storage whose
  * buckets route into one shared buffer.
  *
  * Ordinary 256-bucket aggregation use is covered by `gtest_hash_table.cpp` through
  * `TwoLevelHashMap`, and shares the same class, so nothing here can affect it.
  */

namespace
{

using Cell = HashMapCell<UInt64, UInt64, DefaultHash<UInt64>>;
using Impl = HashMapTable<UInt64, Cell, DefaultHash<UInt64>, TwoLevelHashTableGrower<>, HashTableAllocator>;

/// A real hash for cell placement, so bucket selection can reuse it (`BucketHash = void`).
template <Int32 bits>
using MapWithBits
    = TwoLevelHashTable<UInt64, Cell, DefaultHash<UInt64>, TwoLevelHashTableGrower<>, HashTableAllocator, Impl, bits>;

/// The two bucket counts a JOIN builds with: one bucket for a serial build, 256 for a parallel one.
using SerialMap = MapWithBits<0>;
using ParallelMap = MapWithBits<8>;

/// The `FixedHashMap` shape: an identity placement hash, useless for bucket selection.
struct IdentityHash
{
    [[maybe_unused]] size_t operator()(UInt64 x) const { return x; }
};

ALWAYS_INLINE UInt32 routeWord(UInt64 key)
{
#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return __crc32d(-1U, key);
#else
    return static_cast<UInt32>((key * 0x9E3779B97F4A7C15ULL) >> 32);
#endif
}

ALWAYS_INLINE size_t joinHashRouteSlot(size_t hash, UInt32 route_shift)
{
    return static_cast<size_t>(static_cast<UInt32>(hash)) >> route_shift;
}

struct RouteWordBucketHash
{
    [[maybe_unused]] size_t operator()(UInt64 x) const { return routeWord(x); }
};

using IdentityCell = HashMapCell<UInt64, UInt64, IdentityHash>;
using IdentityImpl = HashMapTable<UInt64, IdentityCell, IdentityHash, TwoLevelHashTableGrower<>, HashTableAllocator>;

using RoutedMap = TwoLevelHashTable<
    UInt64,
    IdentityCell,
    IdentityHash,
    TwoLevelHashTableGrower<>,
    HashTableAllocator,
    IdentityImpl,
    /*bits_for_bucket=*/8,
    /*BucketHash=*/RouteWordBucketHash>;

void insertKeyValue(auto & map, auto key, UInt64 value)
{
    typename std::decay_t<decltype(map)>::LookupResult it = nullptr;
    bool inserted = false;
    map.emplace(key, it, inserted);
    if (inserted)
        new (&it->getMapped()) UInt64(value);
    else
        it->getMapped() = value;
}

/// Exposed so the concurrent tests can take the same external lock `emplace` would need.
template <typename Map>
size_t routedBucket(const Map &, auto key)
{
    return Map::getBucketFromHash(Map::bucketRoutingHash(key, Map::hash(key)));
}

}

TEST(TwoLevelHashTableBuckets, InsertAndFindAcrossBuckets)
{
    constexpr UInt64 num_keys = 100000;

    ParallelMap map;
    ASSERT_EQ(ParallelMap::numBuckets(), 256u);
    ASSERT_TRUE(map.empty());

    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, key, key * 3);

    ASSERT_EQ(map.size(), num_keys);
    ASSERT_FALSE(map.empty());

    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr) << "key " << key << " not found";
        ASSERT_EQ(it->getMapped(), key * 3);
    }

    ASSERT_EQ(map.find(num_keys + 1), nullptr);

    size_t non_empty_buckets = 0;
    for (UInt32 i = 0; i < ParallelMap::numBuckets(); ++i)
        non_empty_buckets += !map.impls[i].empty();
    ASSERT_EQ(non_empty_buckets, ParallelMap::numBuckets());

    size_t iterated = 0;
    for (auto it = map.begin(); it != map.end(); ++it)
        ++iterated;
    ASSERT_EQ(iterated, num_keys);
}

TEST(TwoLevelHashTableBuckets, OneBucketRoutesEverythingToItself)
{
    SerialMap map;
    ASSERT_EQ(SerialMap::numBuckets(), 1u);
    ASSERT_EQ(SerialMap::bucketShift(), 32u);

    for (UInt64 key = 1; key <= 1000; ++key)
        insertKeyValue(map, key, key);

    ASSERT_EQ(map.size(), 1000u);
    for (UInt64 key = 1; key <= 1000; ++key)
        ASSERT_NE(map.find(key), nullptr);

    ASSERT_EQ(map.impls[0].size(), 1000u);
    ASSERT_EQ(SerialMap::getBucketFromHash(0xFFFFFFFFFFFFFFFFULL), 0u);
}

TEST(TwoLevelHashTableBuckets, OneBucketNumbersCellsLikeASingleLevelTable)
{
    /// A serial join replaces a single-level `HashMap` with this, so the cell numbering must match:
    /// the index within the one buffer, with no bucket prefix and nothing to compute first.
    constexpr UInt64 num_keys = 20000;

    SerialMap map;
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, key, key * 9);

    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr) << "key " << key;
        /// Must agree both before and after the prefix sums exist.
        ASSERT_EQ(map.offsetInternal(it), map.impls[0].offsetInternal(it)) << "key " << key;
    }

    map.computeBucketPrefix();
    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_EQ(map.offsetInternalUnsafe(it), map.impls[0].offsetInternal(it)) << "key " << key;
        ASSERT_EQ(map.offsetInternalAtBucket(it, 0), map.impls[0].offsetInternal(it)) << "key " << key;
    }
}

TEST(TwoLevelHashTableBuckets, SizeHintReservesPerBucket)
{
    ParallelMap map(/*size_hint=*/size_t{256} * 1024);

    for (UInt32 i = 0; i < ParallelMap::numBuckets(); ++i)
        ASSERT_GE(map.impls[i].getBufferSizeInCells(), 1024u);
}

TEST(TwoLevelHashTableBuckets, BucketHashDecorrelatesFromIdentityCellHash)
{
    constexpr UInt64 num_keys = 4096;

    RoutedMap map;
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, key, key * 7);

    ASSERT_EQ(map.size(), num_keys);

    /// Bucketing by the identity hash would put these sequential keys in a handful of buckets.
    size_t non_empty_buckets = 0;
    for (UInt32 i = 0; i < RoutedMap::numBuckets(); ++i)
        non_empty_buckets += !map.impls[i].empty();
    ASSERT_GT(non_empty_buckets, 200u) << "routeWord did not decorrelate bucket selection";

    /// Lookups must agree with insertion, or a probe visits a bucket its key never reached.
    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr) << "key " << key << " not found";
        ASSERT_EQ(it->getMapped(), key * 7);
    }
    ASSERT_EQ(map.find(num_keys + 1), nullptr);
}

TEST(TwoLevelHashTableBuckets, EraseUsesBucketRoutingHash)
{
    /// Identity placement hash puts keys 1..4096 in bucket 0 (`hash >> 24`). `erase` must still
    /// visit the routed bucket `find`/`emplace` used, or deletions silently miss.
    constexpr UInt64 num_keys = 4096;

    RoutedMap map;
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, key, key);

    ASSERT_EQ(map.size(), num_keys);

    size_t disagreed = 0;
    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        disagreed += RoutedMap::getBucketFromHash(RoutedMap::hash(key)) != routedBucket(map, key);
        ASSERT_TRUE(map.erase(key)) << "erase missed key " << key;
        ASSERT_EQ(map.find(key), nullptr) << "key " << key << " still present after erase";
    }
    ASSERT_GT(disagreed, 2000u) << "placement hash and BucketHash did not diverge enough to test routing";
    ASSERT_EQ(map.size(), 0u);
    ASSERT_FALSE(map.erase(1));
}

TEST(TwoLevelHashTableBuckets, ConvertingConstructorUsesBucketRoutingHash)
{
    /// Single-level -> two-level copy must route with `BucketHash`, not the placement hash.
    /// Otherwise every key lands in bucket 0 and later `find` looks elsewhere.
    constexpr UInt64 num_keys = 4096;

    IdentityImpl src;
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(src, key, key * 7);

    RoutedMap map(src);
    ASSERT_EQ(map.size(), num_keys);

    size_t non_empty_buckets = 0;
    for (UInt32 i = 0; i < RoutedMap::numBuckets(); ++i)
        non_empty_buckets += !map.impls[i].empty();
    ASSERT_GT(non_empty_buckets, 200u);

    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr) << "key " << key << " missing after conversion";
        ASSERT_EQ(it->getMapped(), key * 7);
    }
}

TEST(TwoLevelHashTableBuckets, IsEmptyCellIsSoundUnderBucketHash)
{
    /// `isEmptyCell` answering true means "no match" without a `find()`, so under a non-void
    /// `BucketHash` - where a hash alone cannot identify the bucket - it must never say true.
    auto routed = std::make_unique<RoutedMap>();
    for (UInt64 key = 1; key <= 1000; ++key)
        insertKeyValue(*routed, key, key);

    for (UInt64 key = 1; key <= 1000; ++key)
        ASSERT_FALSE(routed->isEmptyCell(RoutedMap::hash(key)));
    ASSERT_FALSE(routed->isEmptyCell(RoutedMap::hash(123456789)));

    /// With the default `BucketHash` the hash does identify the bucket, so the fast path stays.
    auto plain = std::make_unique<ParallelMap>();
    for (UInt64 key = 1; key <= 1000; ++key)
        insertKeyValue(*plain, key, key);
    for (UInt64 key = 1; key <= 1000; ++key)
        ASSERT_FALSE(plain->isEmptyCell(ParallelMap::hash(key)));
}

TEST(TwoLevelHashTableBuckets, OffsetInternalIsUniquePerCell)
{
    /// `HashJoin` indexes its RIGHT/FULL flags by these, so they must be distinct across buckets.
    RoutedMap map;
    constexpr UInt64 num_keys = 2000;
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, key, key);

    map.computeBucketPrefix();

    std::unordered_set<size_t> offsets;
    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr);
        const size_t offset = map.offsetInternal(it);
        ASSERT_LE(offset, map.getBufferSizeInCells());
        ASSERT_TRUE(offsets.insert(offset).second) << "duplicate offset " << offset << " for key " << key;
    }
}

TEST(TwoLevelHashTableBuckets, OffsetInternalUnsafeMatchesSafeAfterComputeBucketPrefix)
{
    /// The probe's pattern: compute once at the end of the build, then read offsets with no
    /// per-lookup guard. Both accessors must agree.
    RoutedMap map;
    constexpr UInt64 num_keys = 2000;
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, key, key);

    map.computeBucketPrefix();

    std::unordered_set<size_t> offsets;
    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr);
        const size_t safe_offset = map.offsetInternal(it);
        const size_t unsafe_offset = map.offsetInternalUnsafe(it);
        ASSERT_EQ(safe_offset, unsafe_offset) << "key " << key;
        ASSERT_TRUE(offsets.insert(unsafe_offset).second) << "duplicate offset for key " << key;
    }
}

TEST(TwoLevelHashTableBuckets, ConcurrentBuildWithExternalBucketLocks)
{
    /// The table does not synchronize internally; growth of one bucket must not disturb another.
    constexpr size_t num_threads = 16;
    constexpr UInt64 keys_per_thread = 20000;

    ParallelMap map;
    std::vector<std::mutex> bucket_mutexes(ParallelMap::numBuckets());

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&map, &bucket_mutexes, t]
        {
            const UInt64 begin = t * keys_per_thread + 1;
            for (UInt64 key = begin; key < begin + keys_per_thread; ++key)
            {
                std::lock_guard lock(bucket_mutexes[routedBucket(map, key)]);
                insertKeyValue(map, key, key * 5);
            }
        });
    }
    for (auto & thread : threads)
        thread.join();

    ASSERT_EQ(map.size(), num_threads * keys_per_thread);

    /// Must number every cell a parallel build inserted exactly once.
    map.computeBucketPrefix();
    std::unordered_set<size_t> offsets;
    for (UInt64 key = 1; key <= num_threads * keys_per_thread; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr) << "key " << key << " lost by the concurrent build";
        ASSERT_EQ(it->getMapped(), key * 5) << "mapped value of key " << key << " was corrupted";
        const size_t offset = map.offsetInternalUnsafe(it);
        ASSERT_EQ(offset, map.offsetInternal(it)) << "key " << key;
        ASSERT_TRUE(offsets.insert(offset).second) << "duplicate offset for key " << key;
    }
}

TEST(TwoLevelHashTableBuckets, BucketSelectionMatchesJoinHashRouteSlot)
{
    /// `joinHashRouteSlot` and the table's own routing are written differently -
    /// `(UInt32)h >> (32 - b)` against `(h >> (32 - b)) & (2^b - 1)` - so pin them against each
    /// other, including over the high bits only one of the two sees.
    auto check = []<typename Map>(Map &)
    {
        const auto route_shift = static_cast<UInt32>(32 - std::countr_zero(Map::numBuckets()));
        ASSERT_EQ(Map::bucketShift(), route_shift);

        for (const size_t hash_value : {size_t(0),
                                        size_t(1),
                                        size_t(0xFFFFFFFFULL),
                                        size_t(0x100000000ULL),
                                        size_t(0xFFFFFFFFFFFFFFFFULL),
                                        size_t(0x123456789ABCDEFULL),
                                        size_t(0xDEADBEEF00000000ULL),
                                        size_t(0x00000000DEADBEEFULL)})
        {
            ASSERT_EQ(Map::getBucketFromHash(hash_value), joinHashRouteSlot(hash_value, route_shift))
                << "num_buckets " << Map::numBuckets() << ", hash " << hash_value;
        }
    };

    MapWithBits<0> one_bucket;
    MapWithBits<1> two_buckets;
    MapWithBits<4> sixteen_buckets;
    auto full = std::make_unique<MapWithBits<8>>();
    check(one_bucket);
    check(two_buckets);
    check(sixteen_buckets);
    check(*full);
}

TEST(TwoLevelHashTableBuckets, ReserveSizesEveryBucket)
{
    ParallelMap map;
    map.reserve(ParallelMap::numBuckets() * 2048);

    for (UInt32 i = 0; i < ParallelMap::numBuckets(); ++i)
        ASSERT_GE(map.impls[i].getBufferSizeInCells(), 2048u);

    for (UInt64 key = 1; key <= 10000; ++key)
        insertKeyValue(map, key, key);
    ASSERT_EQ(map.size(), 10000u);
    for (UInt64 key = 1; key <= 10000; ++key)
        ASSERT_NE(map.find(key), nullptr);
}

TEST(TwoLevelHashTableBuckets, ForEachMappedVisitsEveryBucket)
{
    ParallelMap map;
    constexpr UInt64 num_keys = 5000;
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, key, key);

    /// `HashJoin`'s post-build re-ranging rewrites mapped values through this.
    size_t visited = 0;
    map.forEachMapped([&](UInt64 & mapped)
    {
        ++visited;
        mapped *= 2;
    });
    ASSERT_EQ(visited, num_keys);

    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr);
        ASSERT_EQ(it->getMapped(), key * 2);
    }
}

TEST(TwoLevelHashTableBuckets, OffsetsStayValidAfterRecomputingPrefixPostGrowth)
{
    /// The prefix-sum cache does not notice later growth, by design: whoever grows the table
    /// must recompute before handing out offsets again. `StorageJoin` inserts between queries,
    /// and every query recomputes through `reuseJoinedData`'s freeze.
    MapWithBits<4> map;
    for (UInt64 key = 1; key <= 200; ++key)
        insertKeyValue(map, key, key);

    map.computeBucketPrefix();
    for (UInt64 key = 1; key <= 200; ++key)
        ASSERT_GT(map.offsetInternal(map.find(key)), 0u);

    const size_t cells_before = map.getBufferSizeInCells();
    for (UInt64 key = 201; key <= 40000; ++key)
        insertKeyValue(map, key, key);
    ASSERT_GT(map.getBufferSizeInCells(), cells_before) << "test did not actually trigger growth";

    map.computeBucketPrefix();

    std::unordered_set<size_t> offsets;
    for (UInt64 key = 1; key <= 40000; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr);
        const size_t offset = map.offsetInternalUnsafe(it);
        ASSERT_LE(offset, map.getBufferSizeInCells());
        ASSERT_TRUE(offsets.insert(offset).second) << "duplicate offset for key " << key;
    }
}

TEST(TwoLevelHashTableBuckets, ConcurrentBuildWithContendedKeys)
{
    /// Same, but every thread inserts the SAME keys, so they collide inside a bucket too.
    constexpr size_t num_threads = 16;
    constexpr UInt64 num_keys = 5000;

    MapWithBits<6> map;
    std::vector<std::mutex> bucket_mutexes(MapWithBits<6>::numBuckets());

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&map, &bucket_mutexes]
        {
            for (UInt64 key = 1; key <= num_keys; ++key)
            {
                std::lock_guard lock(bucket_mutexes[routedBucket(map, key)]);
                insertKeyValue(map, key, key * 11);
            }
        });
    }
    for (auto & thread : threads)
        thread.join();

    ASSERT_EQ(map.size(), num_keys);
    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr);
        ASSERT_EQ(it->getMapped(), key * 11);
    }
}

TEST(TwoLevelHashTableBuckets, DirectAddressedBucketsRouteIntoOneBuffer)
{
    /// Direct-addressed: every bucket is the one flat table and only names a lock. Memory,
    /// addressing and offsets must be a plain `FixedHashMap`'s, while routing still spreads keys.
    using RangeMap = PartitionedFixedHashMap<UInt16, UInt64, /*size_bits=*/16, /*bits_for_bucket=*/8>;

    RangeMap map;
    ASSERT_EQ(RangeMap::numBuckets(), 256u);

    constexpr UInt64 num_keys = 4096;
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, static_cast<UInt16>(key), key * 3);

    ASSERT_EQ(map.size(), num_keys);

    for (UInt32 i = 1; i < RangeMap::numBuckets(); ++i)
        ASSERT_EQ(map.impls[i].getBufferSizeInBytes(), map.impls[0].getBufferSizeInBytes());
    ASSERT_EQ(map.getBufferSizeInBytes(), map.impls[0].getBufferSizeInBytes());

    /// Or a parallel build would serialize on one lock.
    std::unordered_set<size_t> used_buckets;
    for (UInt64 key = 1; key <= num_keys; ++key)
        used_buckets.insert(routedBucket(map, static_cast<UInt16>(key)));
    ASSERT_GT(used_buckets.size(), 200u) << "keys did not spread over the routing buckets";

    /// Already global - one buffer - but still distinct per populated cell.
    std::unordered_set<size_t> offsets;
    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(static_cast<UInt16>(key));
        ASSERT_NE(it, nullptr) << "key " << key;
        ASSERT_EQ(it->getMapped(), key * 3);
        const size_t offset = map.offsetInternal(it);
        ASSERT_LE(offset, map.getBufferSizeInCells());
        ASSERT_TRUE(offsets.insert(offset).second) << "duplicate offset for key " << key;
    }

    size_t iterated = 0;
    for (auto it = map.begin(); it != map.end(); ++it)
        ++iterated;
    ASSERT_EQ(iterated, num_keys);
}
