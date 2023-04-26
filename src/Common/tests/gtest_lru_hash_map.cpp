#include <iomanip>
#include <iostream>

#include <Common/HashTable/LRUHashMap.h>

#include <gtest/gtest.h>

template<typename LRUHashMap>
std::vector<typename LRUHashMap::Key> convertToVector(const LRUHashMap & map)
{
    std::vector<typename LRUHashMap::Key> result;
    result.reserve(map.size());

    for (auto & node: map)
        result.emplace_back(node.getKey());

    return result;
}

void testInsert(size_t elements_to_insert_size, size_t map_size)
{
    using LRUHashMap = LRUHashMap<int, int>;

    LRUHashMap map(map_size);

    std::vector<int> expected;

    for (size_t i = 0; i < elements_to_insert_size; ++i)
        map.insert(i, i);

    for (size_t i = elements_to_insert_size - map_size; i < elements_to_insert_size; ++i)
        expected.emplace_back(i);

    std::vector<int> actual = convertToVector(map);
    ASSERT_EQ(map.size(), actual.size());
    ASSERT_EQ(actual, expected);
}

TEST(LRUHashMap, Insert)
{
    {
        using LRUHashMap = LRUHashMap<int, int>;

        LRUHashMap map(3);

        map.emplace(1, 1);
        map.insert(2, 2);
        int v = 3;
        map.insert(3, v);
        map.emplace(4, 4);

        std::vector<int> expected = { 2, 3, 4 };
        std::vector<int> actual = convertToVector(map);

        ASSERT_EQ(actual, expected);
    }

    testInsert(1200000, 1200000);
    testInsert(10, 5);
    testInsert(1200000, 2);
    testInsert(1200000, 1);
}

TEST(LRUHashMap, GetModify)
{
    using LRUHashMap = LRUHashMap<int, int>;

    LRUHashMap map(3);

    map.emplace(1, 1);
    map.emplace(2, 2);
    map.emplace(3, 3);

    map.get(3) = 4;

    std::vector<int> expected = { 1, 2, 4 };
    std::vector<int> actual;
    actual.reserve(map.size());

    for (auto & node : map)
        actual.emplace_back(node.getMapped());

    ASSERT_EQ(actual, expected);
}

TEST(LRUHashMap, SetRecentKeyToTop)
{
    using LRUHashMap = LRUHashMap<int, int>;

    LRUHashMap map(3);

    map.emplace(1, 1);
    map.emplace(2, 2);
    map.emplace(3, 3);
    map.emplace(1, 4);

    std::vector<int> expected = { 2, 3, 1 };
    std::vector<int> actual = convertToVector(map);

    ASSERT_EQ(actual, expected);
}

TEST(LRUHashMap, GetRecentKeyToTop)
{
    using LRUHashMap = LRUHashMap<int, int>;

    LRUHashMap map(3);

    map.emplace(1, 1);
    map.emplace(2, 2);
    map.emplace(3, 3);
    map.get(1);

    std::vector<int> expected = { 2, 3, 1 };
    std::vector<int> actual = convertToVector(map);

    ASSERT_EQ(actual, expected);
}

TEST(LRUHashMap, Contains)
{
    using LRUHashMap = LRUHashMap<int, int>;

    LRUHashMap map(3);

    map.emplace(1, 1);
    map.emplace(2, 2);
    map.emplace(3, 3);

    ASSERT_TRUE(map.contains(1));
    ASSERT_TRUE(map.contains(2));
    ASSERT_TRUE(map.contains(3));
    ASSERT_EQ(map.size(), 3);

    map.erase(1);
    map.erase(2);
    map.erase(3);

    ASSERT_EQ(map.size(), 0);
    ASSERT_FALSE(map.contains(1));
    ASSERT_FALSE(map.contains(2));
    ASSERT_FALSE(map.contains(3));
}

TEST(LRUHashMap, Clear)
{
    using LRUHashMap = LRUHashMap<int, int>;

    LRUHashMap map(3);

    map.emplace(1, 1);
    map.emplace(2, 2);
    map.emplace(3, 3);
    map.clear();

    std::vector<int> expected = {};
    std::vector<int> actual = convertToVector(map);

    ASSERT_EQ(actual, expected);
    ASSERT_EQ(map.size(), 0);
}
