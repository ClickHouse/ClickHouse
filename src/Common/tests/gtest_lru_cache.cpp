#include <iomanip>
#include <iostream>
#include <gtest/gtest.h>
#include <Common/LRUCache.h>

TEST(LRUCache, set)
{
    using SimpleLRUCache = DB::LRUCache<int, int>;
    auto lru_cache = SimpleLRUCache(10, 10);
    lru_cache.set(1, std::make_shared<int>(2));
    lru_cache.set(2, std::make_shared<int>(3));

    auto w = lru_cache.weight();
    auto n = lru_cache.count();
    ASSERT_EQ(w, 2);
    ASSERT_EQ(n, 2);
}

TEST(LRUCache, update)
{
    using SimpleLRUCache = DB::LRUCache<int, int>;
    auto lru_cache = SimpleLRUCache(10, 10);
    lru_cache.set(1, std::make_shared<int>(2));
    lru_cache.set(1, std::make_shared<int>(3));
    auto val = lru_cache.get(1);
    ASSERT_TRUE(val != nullptr);
    ASSERT_TRUE(*val == 3);
}

TEST(LRUCache, get)
{
    using SimpleLRUCache = DB::LRUCache<int, int>;
    auto lru_cache = SimpleLRUCache(10, 10);
    lru_cache.set(1, std::make_shared<int>(2));
    lru_cache.set(2, std::make_shared<int>(3));
    SimpleLRUCache::MappedPtr value = lru_cache.get(1);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 2);

    value = lru_cache.get(2);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 3);
}

struct ValueWeight
{
    size_t operator()(const size_t & x) const { return x; }
};

TEST(LRUCache, evictOnSize)
{
    using SimpleLRUCache = DB::LRUCache<int, size_t>;
    auto lru_cache = SimpleLRUCache(20, 3);
    lru_cache.set(1, std::make_shared<size_t>(2));
    lru_cache.set(2, std::make_shared<size_t>(3));
    lru_cache.set(3, std::make_shared<size_t>(4));
    lru_cache.set(4, std::make_shared<size_t>(5));

    auto n = lru_cache.count();
    ASSERT_EQ(n, 3);

    auto value = lru_cache.get(1);
    ASSERT_TRUE(value == nullptr);
}

TEST(LRUCache, evictOnWeight)
{
    using SimpleLRUCache = DB::LRUCache<int, size_t, std::hash<int>, ValueWeight>;
    auto lru_cache = SimpleLRUCache(10, 10);
    lru_cache.set(1, std::make_shared<size_t>(2));
    lru_cache.set(2, std::make_shared<size_t>(3));
    lru_cache.set(3, std::make_shared<size_t>(4));
    lru_cache.set(4, std::make_shared<size_t>(5));

    auto n = lru_cache.count();
    ASSERT_EQ(n, 2);

    auto w = lru_cache.weight();
    ASSERT_EQ(w, 9);

    auto value = lru_cache.get(1);
    ASSERT_TRUE(value == nullptr);
    value = lru_cache.get(2);
    ASSERT_TRUE(value == nullptr);
}

TEST(LRUCache, getOrSet)
{
    using SimpleLRUCache = DB::LRUCache<int, size_t, std::hash<int>, ValueWeight>;
    auto lru_cache = SimpleLRUCache(10, 10);
    size_t x = 10;
    auto load_func = [&] { return std::make_shared<size_t>(x); };
    auto [value, loaded] = lru_cache.getOrSet(1, load_func);
    ASSERT_TRUE(value != nullptr);
    ASSERT_TRUE(*value == 10);
}

