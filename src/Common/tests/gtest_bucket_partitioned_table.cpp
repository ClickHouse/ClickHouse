#include <gtest/gtest.h>

#include <Common/HashTable/BucketPartitionedTable.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>


namespace
{

template <size_t bits>
using TwoLevelMap = TwoLevelHashMap<UInt64, UInt64, DefaultHash<UInt64>, TwoLevelHashTableGrower<>, HashTableAllocator, HashMapTable, bits>;

static_assert(BucketPartitionedMap<TwoLevelMap<0>>);
static_assert(BucketPartitionedMap<TwoLevelMap<8>>);

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

template <typename Map>
typename Map::key_type toKey(UInt64 key)
{
    return static_cast<typename Map::key_type>(key);
}

/// Every populated cell gets its own number.
/// Each number fits the array of `getBufferSizeInCells() + 1` entries that a caller allocates.
template <typename Map>
void assertOffsetsAreUnique(const Map & map, UInt64 first_key, UInt64 last_key)
{
    std::unordered_set<size_t> offsets;
    for (UInt64 key = first_key; key <= last_key; ++key)
    {
        const auto * cell = map.find(toKey<Map>(key));
        ASSERT_NE(cell, nullptr) << "key " << key;
        const size_t offset = map.offsetInternal(cell);
        ASSERT_LE(offset, map.getBufferSizeInCells()) << "key " << key;
        ASSERT_TRUE(offsets.insert(offset).second) << "duplicate offset " << offset << " for key " << key;
    }
}

/// Each insert holds the lock of its bucket, as a concurrent caller would.
/// If `emplace` writes state that all buckets share, the TSan build of the unit tests reports a data race.
template <typename Map>
void fillUnderBucketLocks(Map & map, size_t num_threads, UInt64 keys_per_thread, bool same_keys_in_every_thread)
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
                const auto typed_key = toKey<Map>(key);
                std::lock_guard lock(bucket_mutexes[getBucketOfKey<Map>(typed_key, map.hash(typed_key))]);
                insertKeyValue(map, typed_key, key * 5);
            }
        });
    }
    for (auto & thread : threads)
        thread.join();
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
class BucketPartitionedTableTest : public ::testing::Test
{
};

using Shapes = ::testing::Types<TwoLevelMap<0>, TwoLevelMap<8>>;

}

TYPED_TEST_SUITE(BucketPartitionedTableTest, Shapes);


TYPED_TEST(BucketPartitionedTableTest, InsertAndFind)
{
    using Map = TypeParam;
    constexpr UInt64 num_keys = 5000;

    auto map = std::make_unique<Map>();
    ASSERT_TRUE(map->empty());
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(*map, toKey<Map>(key), key * 3);

    ASSERT_EQ(map->size(), num_keys);
    for (UInt64 key = 1; key <= num_keys; ++key)
    {
        const auto typed_key = toKey<Map>(key);
        auto * cell = map->find(typed_key);
        ASSERT_NE(cell, nullptr) << "key " << key;
        ASSERT_EQ(cell->getMapped(), key * 3) << "key " << key;
        ASSERT_EQ(map->find(typed_key, map->hash(typed_key)), cell) << "key " << key;
    }
    ASSERT_EQ(map->find(toKey<Map>(num_keys + 1)), nullptr);

    size_t visited = 0;
    map->forEachMapped([&](UInt64 &) { ++visited; });
    ASSERT_EQ(visited, num_keys);
}


TYPED_TEST(BucketPartitionedTableTest, OffsetsAreUniqueAfterComputeBucketPrefix)
{
    using Map = TypeParam;
    constexpr UInt64 num_keys = 5000;

    auto map = std::make_unique<Map>();
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(*map, toKey<Map>(key), key);
    map->computeBucketPrefix();
    assertOffsetsAreUnique(*map, 1, num_keys);
}


TYPED_TEST(BucketPartitionedTableTest, IteratorReportsTheBucketOfItsKey)
{
    using Map = TypeParam;
    constexpr UInt64 num_keys = 5000;

    auto map = std::make_unique<Map>();
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(*map, toKey<Map>(key), key);

    std::vector<char> seen(num_keys + 1, 0);
    for (auto it = map->begin(); it != map->end(); ++it)
    {
        const auto key = it->getKey();
        ASSERT_EQ(it.getBucket(), getBucketOfKey<Map>(key, map->hash(key))) << "key " << key;
        ASSERT_FALSE(seen[key]) << "key " << key << " visited twice";
        seen[key] = 1;
    }
    ASSERT_TRUE(std::all_of(seen.begin() + 1, seen.end(), [](char c) { return c == 1; }));

    const Map & const_map = *map;
    size_t iterated = 0;
    for (auto it = const_map.begin(); it != const_map.end(); ++it)
    {
        const auto key = it->getKey();
        ASSERT_EQ(it.getBucket(), getBucketOfKey<Map>(key, const_map.hash(key))) << "key " << key;
        ++iterated;
    }
    ASSERT_EQ(iterated, num_keys);
}


TYPED_TEST(BucketPartitionedTableTest, ConcurrentFillUnderOneLockPerBucket)
{
    using Map = TypeParam;
    constexpr size_t num_threads = 16;
    constexpr UInt64 keys_per_thread = 4000;

    /// With distinct keys, no two threads write the same cell. With the same keys, the threads collide inside each bucket.
    for (const bool same_keys_in_every_thread : {false, true})
    {
        auto map = std::make_unique<Map>();
        fillUnderBucketLocks(*map, num_threads, keys_per_thread, same_keys_in_every_thread);

        const UInt64 total_keys = same_keys_in_every_thread ? keys_per_thread : num_threads * keys_per_thread;
        ASSERT_EQ(map->size(), total_keys);
        for (UInt64 key = 1; key <= total_keys; ++key)
        {
            const auto * cell = map->find(toKey<Map>(key));
            ASSERT_NE(cell, nullptr) << "key " << key << " was lost";
            ASSERT_EQ(cell->getMapped(), key * 5) << "key " << key;
        }
    }
}


TEST(TwoLevelHashTableBuckets, OneBucketNeedsNoPrefix)
{
    constexpr UInt64 num_keys = 20000;

    auto map = std::make_unique<TwoLevelMap<0>>();
    for (UInt64 key = 1; key <= num_keys; ++key)
        insertKeyValue(*map, key, key);

    /// With one bucket, the number is the bucket's own `offsetInternal`, before and after `computeBucketPrefix`.
    for (int pass = 0; pass < 2; ++pass)
    {
        for (UInt64 key = 1; key <= num_keys; ++key)
        {
            const auto * cell = map->find(key);
            ASSERT_NE(cell, nullptr) << "key " << key;
            ASSERT_EQ(map->offsetInternal(cell), map->impls[0].offsetInternal(cell)) << "key " << key << ", pass " << pass;
        }
        map->computeBucketPrefix();
    }
}


TEST(TwoLevelHashTableBuckets, OffsetsAreValidAgainAfterGrowthAndRecompute)
{
    auto map = std::make_unique<TwoLevelMap<4>>();
    for (UInt64 key = 1; key <= 200; ++key)
        insertKeyValue(*map, key, key);
    map->computeBucketPrefix();
    assertOffsetsAreUnique(*map, 1, 200);

    const size_t cells_before = map->getBufferSizeInCells();
    for (UInt64 key = 201; key <= 40000; ++key)
        insertKeyValue(*map, key, key);
    ASSERT_GT(map->getBufferSizeInCells(), cells_before) << "the inserts did not grow any bucket";

    map->computeBucketPrefix();
    assertOffsetsAreUnique(*map, 1, 40000);
}
