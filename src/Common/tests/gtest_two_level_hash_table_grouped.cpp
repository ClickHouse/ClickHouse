#include <gtest/gtest.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/TwoLevelHashMap.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace
{

/// The default constructor enables the grouped growth regardless of `CLICKHOUSE_TWO_LEVEL_GROUPED`.
/// The inherited constructors (the size hint and the conversion from a single-level table) read the knob, as in production.
template <typename Base>
struct Grouped : Base
{
    using Base::Base;

    Grouped() { this->grouped_growth = true; }

    bool bucketInArena(size_t bucket) { return this->isBucketInArena(bucket); }
    [[nodiscard]] bool bucketAtFillLimit(size_t bucket) const { return this->isBucketAtFillLimit(bucket); }
    static constexpr bool supports() { return Base::supports_grouped_growth; }
};

using Map = Grouped<TwoLevelHashMap<UInt64, UInt64, DefaultHash<UInt64>, TwoLevelHashTableGrower<>, TwoLevelHashTableGroupedAllocator>>;
/// With fewer buckets, the buckets reach the Robin Hood mode with fewer elements.
using Map16 = Grouped<TwoLevelHashMap<UInt64, UInt64, DefaultHash<UInt64>, TwoLevelHashTableGrower<>, TwoLevelHashTableGroupedAllocator, HashMapTable, 4>>;
using Set = Grouped<TwoLevelHashSet<UInt64, DefaultHash<UInt64>, TwoLevelHashTableGrower<>, TwoLevelHashTableGroupedAllocator>>;

static_assert(Map::supports());
static_assert(Map16::supports());
static_assert(Set::supports());

/// The grouped growth is opt-in: the default types keep the plain allocator.
static_assert(!Grouped<TwoLevelHashMap<UInt64, UInt64>>::supports());
static_assert(!Grouped<TwoLevelHashSet<UInt64>>::supports());

std::vector<UInt64> randomKeys(size_t n, UInt64 seed)
{
    std::mt19937_64 rng(seed);
    std::vector<UInt64> keys(n);
    for (auto & key : keys)
        key = rng() % (n * 4);
    keys[0] = 0; /// The zero key is stored separately.
    return keys;
}

template <typename TwoLevel>
void checkSameBufferSize(TwoLevel & map)
{
    for (size_t i = 1; i < TwoLevel::NUM_BUCKETS; ++i)
        ASSERT_EQ(map.impls[i].getBufferSizeInCells(), map.impls[0].getBufferSizeInCells());
}

}


template <typename Map>
void testInsertFindIterate(std::initializer_list<size_t> sizes)
{
    for (size_t n : sizes)
    {
        auto keys = randomKeys(n, n);
        std::unordered_map<UInt64, UInt64> reference;
        auto map_holder = std::make_unique<Map>();
        auto & map = *map_holder;
        for (UInt64 key : keys)
        {
            map[key] += key + 1;
            reference[key] += key + 1;
        }

        ASSERT_EQ(map.size(), reference.size());
        for (const auto & [key, value] : reference)
        {
            auto * it = map.find(key);
            ASSERT_NE(it, nullptr) << key;
            ASSERT_EQ(it->getMapped(), value);
        }
        ASSERT_EQ(map.find(n * 4 + 1), nullptr);

        size_t iterated = 0;
        for (auto it = map.begin(); it != map.end(); ++it)
            ++iterated;
        ASSERT_EQ(iterated, reference.size());

        if (n >= 100'000)
        {
            checkSameBufferSize(map);
            for (size_t i = 0; i < Map::NUM_BUCKETS; ++i)
                ASSERT_TRUE(map.bucketInArena(i));
        }
    }
}

TEST(TwoLevelHashTableGrouped, InsertFindIterate)
{
    testInsertFindIterate<Map>({10, 1000, 100'000, 1'000'000});
    /// Crosses the 4x growth steps, the switch to the Robin Hood mode and the 2x growth steps after it.
    testInsertFindIterate<Map16>({3'000'000});
}

TEST(TwoLevelHashTableGrouped, Set)
{
    auto keys = randomKeys(500'000, 1);
    std::unordered_set<UInt64> reference(keys.begin(), keys.end());
    auto set_holder = std::make_unique<Set>();
    auto & set = *set_holder;
    for (UInt64 key : keys)
        set.insert(key);
    ASSERT_EQ(set.size(), reference.size());
    for (UInt64 key : reference)
        ASSERT_NE(set.find(key), nullptr);
    checkSameBufferSize(set);
}

namespace
{

void require(bool condition, const char * what)
{
    if (!condition)
    {
        std::cerr << "Check failed: " << what << std::endl;
        std::_Exit(1);
    }
}

/// Runs in a fresh process, so that `CLICKHOUSE_TWO_LEVEL_GROUPED` is read for the first time here.
[[noreturn]] void checkProductionConstructors()
{
    setenv("CLICKHOUSE_TWO_LEVEL_GROUPED", "1", 1); // NOLINT(concurrency-mt-unsafe)
    require(twoLevelHashTableGroupedGrowthEnabled(), "the knob enables the grouped growth");

    auto keys = randomKeys(200'000, 2);
    HashMap<UInt64, UInt64> single_level;
    for (UInt64 key : keys)
        single_level[key] = key * 3 + 1;
    require(single_level.has(0), "the source has the zero key");

    auto all_in_arena = [](Map & map)
    {
        for (size_t i = 0; i < Map::NUM_BUCKETS; ++i)
            if (!map.bucketInArena(i) || map.impls[i].getBufferSizeInCells() != map.impls[0].getBufferSizeInCells())
                return false;
        return true;
    };

    /// The converting constructor, as in `AggregatedDataVariants::convertToTwoLevel`.
    {
        auto map = std::make_unique<Map>(single_level);
        require(map->size() == single_level.size(), "the converted table has all keys");
        for (auto it = single_level.begin(); it != single_level.end(); ++it)
        {
            const auto * cell = map->find(it->getKey());
            require(cell && cell->getMapped() == it->getKey() * 3 + 1, "the converted table has the same values");
        }
        const auto * zero = map->find(0);
        require(zero && zero->getMapped() == 1, "the converted table has the zero key");
        require(map->find(keys.size() * 4 + 1) == nullptr, "the converted table has no extra keys");

        size_t iterated = 0;
        for (auto it = map->begin(); it != map->end(); ++it)
            ++iterated;
        require(iterated == single_level.size(), "the iteration over the converted table visits every key once");
        require(all_in_arena(*map), "the buckets of the converted table are in the shared area and have the same size");

        /// The table keeps growing together after the conversion.
        for (UInt64 key = 1; key <= 2'000'000; ++key)
            (*map)[(1ULL << 40) + key] = key;
        require(map->size() == single_level.size() + 2'000'000, "the converted table accepts new keys");
        require(all_in_arena(*map), "the buckets are still in the shared area after more inserts");
        require(map->find(0)->getMapped() == 1, "the zero key survives the growth");
    }

    /// The constructor with a size hint.
    {
        const size_t default_cells = std::make_unique<Map>()->impls[0].getBufferSizeInCells();

        auto map = std::make_unique<Map>(1'000'000);
        require(map->empty(), "the table with a size hint is empty");
        require(all_in_arena(*map), "the size hint preallocates the buckets in the shared area");
        const size_t cells = map->impls[0].getBufferSizeInCells();
        require(cells > default_cells, "the size hint preallocates more than the initial size");
        for (auto it = single_level.begin(); it != single_level.end(); ++it)
            (*map)[it->getKey()] = it->getMapped();
        require(map->size() == single_level.size(), "the preallocated table has all keys");
        for (auto it = single_level.begin(); it != single_level.end(); ++it)
        {
            const auto * cell = map->find(it->getKey());
            require(cell && cell->getMapped() == it->getKey() * 3 + 1, "the preallocated table has the same values");
        }
        /// 200 thousand keys fit into the buckets preallocated for a million.
        require(map->impls[0].getBufferSizeInCells() == cells, "the preallocated buckets did not grow");
        require(all_in_arena(*map), "the preallocated buckets stay in the shared area");
    }

    std::_Exit(0);
}

}

/// `AggregatedDataVariants::convertToTwoLevel` and the initialization from a size hint construct the table
/// with these constructors, and they read `CLICKHOUSE_TWO_LEVEL_GROUPED`.
TEST(TwoLevelHashTableGroupedDeathTest, ConvertFromSingleLevelAndSizeHint)
{
    /// The `threadsafe` style re-executes the test binary, so the variable is read for the first time in the child.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkProductionConstructors(), ::testing::ExitedWithCode(0), ".*");
}

TEST(TwoLevelHashTableGrouped, BucketsLeaveTheArena)
{
    auto keys = randomKeys(100'000, 3);
    std::unordered_map<UInt64, UInt64> reference;
    auto map = std::make_unique<Map>();
    for (UInt64 key : keys)
    {
        (*map)[key] = key;
        reference[key] = key;
    }
    ASSERT_TRUE(map->bucketInArena(5));

    /// A bucket that grows on its own moves to a private buffer.
    auto & bucket = map->impls[5];
    const size_t cells = bucket.getBufferSizeInCells();
    for (UInt64 key = 1; bucket.getBufferSizeInCells() == cells; ++key)
    {
        UInt64 big_key = (1ULL << 40) + key;
        size_t hash_value = map->hash(big_key);
        if (Map::getBucketFromHash(hash_value) != 5)
            continue;
        Map::LookupResult it = nullptr;
        bool inserted = false;
        bucket.emplace(big_key, it, inserted, hash_value);
        it->getMapped() = big_key;
        reference[big_key] = big_key;
    }
    ASSERT_FALSE(map->bucketInArena(5));
    ASSERT_TRUE(map->bucketInArena(6));

    /// A bucket moved to another table keeps the arena alive after the table is destroyed.
    auto other = std::make_unique<Map>();
    other->impls[6] = std::move(map->impls[6]);
    std::swap(map->impls[7], map->impls[8]);
    std::swap(map->impls[7], map->impls[8]);
    map.reset();

    for (auto it = other->impls[6].begin(); it != other->impls[6].end(); ++it)
        ASSERT_EQ(it->getMapped(), reference.at(it->getKey()));
}

TEST(TwoLevelHashTableGrouped, ExistingKeyAtFillLimitDoesNotGrow)
{
    auto map = std::make_unique<Map>();
    auto & bucket = map->impls[3];

    /// Fill bucket 3 exactly to the limit at which the next new key would grow all buckets.
    std::vector<UInt64> bucket_keys;
    for (UInt64 key = 1; !map->bucketAtFillLimit(3); ++key)
    {
        size_t hash_value = map->hash(key);
        if (Map::getBucketFromHash(hash_value) != 3)
            continue;
        (*map)[key] = key;
        bucket_keys.push_back(key);
    }
    ASSERT_FALSE(bucket_keys.empty());

    const size_t cells = bucket.getBufferSizeInCells();
    const auto * buf = bucket.begin().getPtr();

    /// Inserting existing keys must neither reallocate nor grow any bucket.
    for (int round = 0; round < 3; ++round)
    {
        for (UInt64 key : bucket_keys)
        {
            Map::LookupResult it = nullptr;
            bool inserted = true;
            map->emplace(key, it, inserted);
            ASSERT_FALSE(inserted);
            ASSERT_EQ(it->getMapped(), key);
        }
    }
    ASSERT_EQ(bucket.getBufferSizeInCells(), cells);
    ASSERT_EQ(bucket.begin().getPtr(), buf);
    ASSERT_EQ(map->size(), bucket_keys.size());

    /// A new key still grows all buckets together.
    for (UInt64 key = (1ULL << 40);; ++key)
    {
        size_t hash_value = map->hash(key);
        if (Map::getBucketFromHash(hash_value) != 3)
            continue;
        (*map)[key] = key;
        break;
    }
    ASSERT_GT(bucket.getBufferSizeInCells(), cells);
    checkSameBufferSize(*map);
}


/// A table can be filled by several threads, with different buckets at the same time, each under the lock of its bucket.
/// It must call `disableGroupedGrowth` first: otherwise `growAllBuckets` moves the buckets that other threads are writing.
/// The TSan build of the unit tests reports a data race if the buckets still grow together.
TEST(TwoLevelHashTableGrouped, ConcurrentFillAfterDisableGroupedGrowth)
{
    constexpr size_t num_threads = 16;
    constexpr UInt64 keys_per_thread = 20000;

    auto map_holder = std::make_unique<Map>();
    auto & map = *map_holder;
    map.disableGroupedGrowth();

    std::vector<std::mutex> bucket_mutexes(Map::NUM_BUCKETS);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
    {
        const UInt64 first_key = t * keys_per_thread + 1;
        threads.emplace_back([&map, &bucket_mutexes, first_key]
        {
            for (UInt64 key = first_key; key < first_key + keys_per_thread; ++key)
            {
                const size_t hash_value = map.hash(key);
                std::lock_guard lock(bucket_mutexes[Map::getBucketFromHash(hash_value)]);
                Map::LookupResult it = nullptr;
                bool inserted = false;
                map.emplace(key, it, inserted, hash_value);
                it->getMapped() = key * 5;
            }
        });
    }
    for (auto & thread : threads)
        thread.join();

    ASSERT_EQ(map.size(), num_threads * keys_per_thread);
    for (UInt64 key = 1; key <= num_threads * keys_per_thread; ++key)
    {
        const auto * cell = map.find(key);
        ASSERT_NE(cell, nullptr) << "key " << key << " was lost";
        ASSERT_EQ(cell->getMapped(), key * 5) << "key " << key;
    }
    for (size_t i = 0; i < Map::NUM_BUCKETS; ++i)
        ASSERT_FALSE(map.bucketInArena(i));
}
