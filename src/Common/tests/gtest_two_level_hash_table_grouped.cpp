#include <gtest/gtest.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/TwoLevelHashMap.h>

#include <memory>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace
{

/// Enables the grouped growth regardless of `CLICKHOUSE_TWO_LEVEL_GROUPED`.
template <typename Base>
struct Grouped : Base
{
    using Base::Base;

    Grouped() { this->grouped_growth = true; }

    template <typename Source>
    requires(!std::is_arithmetic_v<Source>)
    explicit Grouped(const Source & src, bool) : Base()
    {
        this->grouped_growth = true;
        for (auto it = src.begin(); it != src.end(); ++it)
            this->insert(it->getValue());
    }

    bool bucketInArena(size_t bucket) { return this->isBucketInArena(bucket); }
    static constexpr bool supports() { return Base::supports_grouped_growth; }
};

using Map = Grouped<TwoLevelHashMap<UInt64, UInt64>>;
/// With fewer buckets, the buckets reach the Robin Hood mode with fewer elements.
using Map16 = Grouped<TwoLevelHashMap<UInt64, UInt64, DefaultHash<UInt64>, TwoLevelHashTableGrower<>, HashTableAllocator, HashMapTable, 4>>;
using Set = Grouped<TwoLevelHashSet<UInt64>>;

static_assert(Map::supports());
static_assert(Map16::supports());
static_assert(Set::supports());

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

TEST(TwoLevelHashTableGrouped, ConvertFromSingleLevelAndSizeHint)
{
    auto keys = randomKeys(200'000, 2);
    HashMap<UInt64, UInt64> single_level;
    for (UInt64 key : keys)
        single_level[key] = key * 3;

    /// The converting constructor of `TwoLevelHashTable` reads the knob, so check the same path through `insert`.
    auto map = std::make_unique<Map>(single_level, true);
    ASSERT_EQ(map->size(), single_level.size());
    for (auto it = single_level.begin(); it != single_level.end(); ++it)
        ASSERT_EQ(map->find(it->getKey())->getMapped(), it->getKey() * 3);

    auto with_hint = std::make_unique<Map>(1'000'000);
    ASSERT_EQ(with_hint->size(), 0);
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
