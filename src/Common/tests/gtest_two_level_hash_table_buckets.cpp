#include <gtest/gtest.h>

#include <Common/HashTable/BucketPartitionedTable.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/PartitionedFixedHashMap.h>
#include <Common/HashTable/PartitionedFixedHashSet.h>
#include <Common/HashTable/TwoLevelHashMap.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>


/** Covers `TwoLevelHashTable` beyond its default shape.
  * That includes a bucket count other than 256, a bucket hash that differs from the cell hash,
  * and the table-wide cell numbering of `offsetInternal`.
  * The 256-bucket shape that aggregation uses is the same class with the default arguments.
  */

namespace
{

template <size_t bits>
using MapWithBits = TwoLevelHashMap<UInt64, UInt64, DefaultHash<UInt64>, TwoLevelHashTableGrower<>, HashTableAllocator, HashMapTable, bits>;

using OneBucketMap = MapWithBits<0>;
using DefaultMap = MapWithBits<8>;

/// A placement hash that is useless for bucket selection: sequential keys share their high bits.
struct IdentityHash
{
    size_t operator()(UInt64 x) const { return x; }
};

struct MixingBucketHash
{
    size_t operator()(UInt64 x) const { return static_cast<UInt32>((x * 0x9E3779B97F4A7C15ULL) >> 32); }
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
    /* BITS_FOR_BUCKET = */ 8,
    MixingBucketHash>;

static_assert(BucketPartitionedMap<OneBucketMap>);
static_assert(BucketPartitionedMap<DefaultMap>);
static_assert(BucketPartitionedMap<RoutedMap>);
static_assert(BucketPartitionedMap<PartitionedFixedHashMap<UInt16, UInt64>>);
static_assert(BucketPartitionedTable<PartitionedFixedHashSet<UInt16>>);

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
size_t countNonEmptyBuckets(const Map & map)
{
    size_t res = 0;
    for (UInt32 i = 0; i < Map::NUM_BUCKETS; ++i)
        res += !map.impls[i].empty();
    return res;
}

template <typename Map>
size_t countByIteration(const Map & map)
{
    size_t res = 0;
    for (typename Map::const_iterator it = map.begin(); it != map.end(); ++it)
        ++res;
    return res;
}

/// Every populated cell must get its own number, and none may exceed the array size a caller would allocate.
template <typename Map>
void assertOffsetsAreUnique(const Map & map, UInt64 first_key, UInt64 last_key)
{
    std::unordered_set<size_t> offsets;
    for (UInt64 key = first_key; key <= last_key; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr) << "key " << key;
        const size_t offset = map.offsetInternal(it);
        ASSERT_LE(offset, map.getBufferSizeInCells()) << "key " << key;
        ASSERT_TRUE(offsets.insert(offset).second) << "duplicate offset " << offset << " for key " << key;
    }
}

/// One lock per bucket, as a concurrent caller would do. A race here shows up under TSan, which the
/// unit test CI job runs with.
template <typename Map>
void fillConcurrently(Map & map, size_t num_threads, UInt64 keys_per_thread, bool same_keys_in_every_thread)
{
    std::vector<std::mutex> bucket_mutexes(Map::NUM_BUCKETS);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
    {
        const UInt64 first_key = same_keys_in_every_thread ? 1 : t * keys_per_thread + 1;
        threads.emplace_back([&map, &bucket_mutexes, first_key, keys_per_thread]
        {
            for (UInt64 key = first_key; key < first_key + keys_per_thread; ++key)
            {
                std::lock_guard lock(bucket_mutexes[routedBucket<Map>(key)]);
                insertKeyValue(map, key, key * 5);
            }
        });
    }
    for (auto & thread : threads)
        thread.join();
}

}


TEST(TwoLevelHashTableBuckets, InsertFindIterateAcrossBuckets)
{
    constexpr UInt64 num_keys = 100000;

    DefaultMap map;
    ASSERT_EQ(DefaultMap::NUM_BUCKETS, 256u);
    ASSERT_TRUE(map.empty());

    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, key, key * 3);

    ASSERT_EQ(map.size(), num_keys);
    ASSERT_FALSE(map.empty());
    ASSERT_EQ(countNonEmptyBuckets(map), DefaultMap::NUM_BUCKETS);
    ASSERT_EQ(countByIteration(map), num_keys);

    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr) << "key " << key;
        ASSERT_EQ(it->getMapped(), key * 3);
        ASSERT_TRUE(map.has(key));
    }
    ASSERT_EQ(map.find(num_keys + 1), nullptr);
    ASSERT_FALSE(map.has(num_keys + 1));
}


TEST(TwoLevelHashTableBuckets, OneBucketBehavesLikeASingleLevelTable)
{
    constexpr UInt64 num_keys = 20000;

    OneBucketMap map;
    ASSERT_EQ(OneBucketMap::NUM_BUCKETS, 1u);
    ASSERT_EQ(OneBucketMap::bucketShift(), 32u);
    ASSERT_EQ(OneBucketMap::getBucketFromHash(0xFFFFFFFFFFFFFFFFULL), 0u);

    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, key, key * 9);

    ASSERT_EQ(map.size(), num_keys);
    ASSERT_EQ(map.impls[0].size(), num_keys);

    /// The numbering is the one bucket's own, and needs no `computeBucketPrefix`; the second pass
    /// checks that computing them changes nothing.
    for (int pass = 0; pass < 2; ++pass)
    {
        for (UInt64 key = 1; key <= num_keys; ++key)
        {
            auto * it = map.find(key);
            ASSERT_NE(it, nullptr) << "key " << key;
            ASSERT_EQ(map.offsetInternal(it), map.impls[0].offsetInternal(it)) << "key " << key;
            ASSERT_EQ(map.offsetInternalAtBucket(it, 0), map.impls[0].offsetInternal(it)) << "key " << key;
        }
        map.computeBucketPrefix();
    }
}


TEST(TwoLevelHashTableBuckets, SizeHintAndReserveSizeEveryBucket)
{
    /// Two 256-bucket tables do not fit the 64 KB frame limit of the test, hence the heap.
    auto hinted = std::make_unique<DefaultMap>(/* size_hint = */ 256uz * 1024);
    for (UInt32 i = 0; i < DefaultMap::NUM_BUCKETS; ++i)
        ASSERT_GE(hinted->impls[i].getBufferSizeInCells(), 1024u) << "bucket " << i;

    auto reserved = std::make_unique<DefaultMap>();
    reserved->reserve(256uz * 2048);
    for (UInt32 i = 0; i < DefaultMap::NUM_BUCKETS; ++i)
        ASSERT_GE(reserved->impls[i].getBufferSizeInCells(), 2048u) << "bucket " << i;

    for (UInt64 key = 1; key <= 10000; ++key)
        insertKeyValue(*reserved, key, key);
    ASSERT_EQ(reserved->size(), 10000u);
}


TEST(TwoLevelHashTableBuckets, BucketIsTakenFromTheHighEndOfTheLow32Bits)
{
    /// A caller that routes keys before it has a table computes the bucket itself.
    /// The formula is part of the interface: the top `bits` of the low 32 bits of the hash.
    const auto check = []<size_t bits>()
    {
        using Map = MapWithBits<bits>;
        ASSERT_EQ(Map::bucketShift(), static_cast<UInt32>(32 - bits));
        for (const size_t hash_value : {size_t(0), size_t(1), size_t(0xFFFFFFFFULL), size_t(0x100000000ULL),
                                        size_t(0xFFFFFFFFFFFFFFFFULL), size_t(0x123456789ABCDEFULL), size_t(0xDEADBEEF00000000ULL)})
        {
            size_t expected = 0;
            if constexpr (bits != 0)
                expected = static_cast<UInt32>(hash_value) >> (32 - bits);
            ASSERT_EQ(Map::getBucketFromHash(hash_value), expected) << "bits " << bits << ", hash " << hash_value;
        }
    };
    check.template operator()<0>();
    check.template operator()<1>();
    check.template operator()<4>();
    check.template operator()<8>();
}


TEST(TwoLevelHashTableBuckets, BucketHashRoutesInsertFindAndErase)
{
    constexpr UInt64 num_keys = 4096;

    RoutedMap map;
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, key, key * 7);
    ASSERT_EQ(map.size(), num_keys);

    /// Routing on the identity hash would put keys 1..4096 into a couple of buckets.
    ASSERT_GT(countNonEmptyBuckets(map), 200u);

    size_t keys_routed_away_from_cell_hash = 0;
    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        keys_routed_away_from_cell_hash += RoutedMap::getBucketFromHash(RoutedMap::hash(key)) != routedBucket<RoutedMap>(key);
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr) << "key " << key;
        ASSERT_EQ(it->getMapped(), key * 7);
    }
    ASSERT_GT(keys_routed_away_from_cell_hash, 2000u) << "the two hashes agreed too often to test routing";
    ASSERT_EQ(map.find(num_keys + 1), nullptr);

    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        ASSERT_TRUE(map.erase(key)) << "erase looked in the wrong bucket for key " << key;
        ASSERT_EQ(map.find(key), nullptr) << "key " << key;
    }
    ASSERT_EQ(map.size(), 0u);
    ASSERT_FALSE(map.erase(1));
}


TEST(TwoLevelHashTableBuckets, ConvertingConstructorRoutesByBucketHash)
{
    constexpr UInt64 num_keys = 4096;

    IdentityImpl single_level;
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(single_level, key, key * 7);

    RoutedMap map(single_level);
    ASSERT_EQ(map.size(), num_keys);
    ASSERT_GT(countNonEmptyBuckets(map), 200u);
    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr) << "key " << key;
        ASSERT_EQ(it->getMapped(), key * 7);
    }
}


TEST(TwoLevelHashTableBuckets, HashOnlyQueriesStayConservativeUnderBucketHash)
{
    /// `isEmptyCell` answering true means "no match" without a lookup. A bucket hash makes the cell
    /// hash insufficient to find the bucket, so the answer must then always be false.
    auto routed = std::make_unique<RoutedMap>();
    for (UInt64 key = 1; key <= 1000; ++key)
        insertKeyValue(*routed, key, key);
    for (UInt64 key = 1; key <= 1000; ++key)
        ASSERT_FALSE(routed->isEmptyCell(RoutedMap::hash(key)));
    ASSERT_FALSE(routed->isEmptyCell(RoutedMap::hash(123456789)));

    /// Without a bucket hash the fast path stays: an empty table answers true, a present key false.
    auto plain = std::make_unique<DefaultMap>();
    ASSERT_TRUE(plain->isEmptyCell(DefaultMap::hash(1)));
    for (UInt64 key = 1; key <= 1000; ++key)
        insertKeyValue(*plain, key, key);
    for (UInt64 key = 1; key <= 1000; ++key)
        ASSERT_FALSE(plain->isEmptyCell(DefaultMap::hash(key)));
}


TEST(TwoLevelHashTableBuckets, OffsetsAreUniqueAcrossBuckets)
{
    constexpr UInt64 num_keys = 2000;

    /// With a bucket hash the bucket of a cell comes from the key, not from the cell hash.
    auto routed = std::make_unique<RoutedMap>();
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(*routed, key, key);
    routed->computeBucketPrefix();
    assertOffsetsAreUnique(*routed, 1, num_keys);

    auto plain = std::make_unique<DefaultMap>();
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(*plain, key, key);
    plain->computeBucketPrefix();
    assertOffsetsAreUnique(*plain, 1, num_keys);
}


TEST(TwoLevelHashTableBuckets, OffsetsAreValidAgainAfterGrowthAndRecompute)
{
    MapWithBits<4> map;
    for (UInt64 key = 1; key <= 200; ++key)
        insertKeyValue(map, key, key);
    map.computeBucketPrefix();
    assertOffsetsAreUnique(map, 1, 200);

    const size_t cells_before = map.getBufferSizeInCells();
    for (UInt64 key = 201; key <= 40000; ++key)
        insertKeyValue(map, key, key);
    ASSERT_GT(map.getBufferSizeInCells(), cells_before) << "the inserts did not grow any bucket";

    map.computeBucketPrefix();
    assertOffsetsAreUnique(map, 1, 40000);
}


TEST(TwoLevelHashTableBuckets, ConcurrentInsertsUnderOneLockPerBucket)
{
    constexpr size_t num_threads = 16;
    constexpr UInt64 keys_per_thread = 20000;

    DefaultMap map;
    fillConcurrently(map, num_threads, keys_per_thread, /* same_keys_in_every_thread = */ false);

    ASSERT_EQ(map.size(), num_threads * keys_per_thread);
    for (UInt64 key = 1; key <= num_threads * keys_per_thread; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr) << "key " << key << " was lost";
        ASSERT_EQ(it->getMapped(), key * 5) << "key " << key;
    }
    map.computeBucketPrefix();
    assertOffsetsAreUnique(map, 1, num_threads * keys_per_thread);
}


TEST(TwoLevelHashTableBuckets, ConcurrentInsertsOfTheSameKeysCollideInsideBuckets)
{
    constexpr size_t num_threads = 16;
    constexpr UInt64 num_keys = 5000;

    MapWithBits<6> map;
    fillConcurrently(map, num_threads, num_keys, /* same_keys_in_every_thread = */ true);

    ASSERT_EQ(map.size(), num_keys);
    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        auto * it = map.find(key);
        ASSERT_NE(it, nullptr) << "key " << key;
        ASSERT_EQ(it->getMapped(), key * 5) << "key " << key;
    }
}


TEST(TwoLevelHashTableBuckets, ForEachMappedVisitsEveryBucket)
{
    constexpr UInt64 num_keys = 5000;

    DefaultMap map;
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(map, key, key);

    size_t visited = 0;
    map.forEachMapped([&](UInt64 & mapped)
    {
        ++visited;
        mapped *= 2;
    });
    ASSERT_EQ(visited, num_keys);

    for (UInt64 key = 1; key <= num_keys; ++key)
        ASSERT_EQ(map.find(key)->getMapped(), key * 2) << "key " << key;
}


TEST(TwoLevelHashTableBuckets, WriteAndReadRoundTripEveryBucket)
{
    using Map = MapWithBits<4>;
    ASSERT_EQ(Map::serializedPartitionCount(), 16u);

    Map source;
    for (UInt64 key = 1; key <= 3000; ++key)
        insertKeyValue(source, key, key * 3);

    DB::WriteBufferFromOwnString wb;
    source.write(wb);

    Map copy;
    DB::ReadBufferFromString rb(wb.str());
    copy.read(rb);
    ASSERT_TRUE(rb.eof());

    ASSERT_EQ(copy.size(), source.size());
    for (UInt64 key = 1; key <= 3000; ++key)
    {
        auto * it = copy.find(key);
        ASSERT_NE(it, nullptr) << "key " << key;
        ASSERT_EQ(it->getMapped(), key * 3);
    }
}
