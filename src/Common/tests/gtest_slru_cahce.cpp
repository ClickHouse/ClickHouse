#include <iomanip>
#include <iostream>
#include <gtest/gtest.h>
#include <Common/SLRUCache.h>

TEST(SLRUCache, set)
{
    using SimpleSLRUCache = DB::SLRUCache<int, int>;
    auto slru_cache = SimpleSLRUCache(/*max_protected_size=*/5, /*max_total_size=*/10);
    slru_cache.set(1, std::make_shared<int>(2));
    slru_cache.set(2, std::make_shared<int>(3));

    auto w = slru_cache.weight();
    auto n = slru_cache.count();
    ASSERT_EQ(w, 2);
    ASSERT_EQ(n, 2);
}

TEST(SLRUCache, update)
{
    using SimpleSLRUCache = DB::SLRUCache<int, int>;
    auto slru_cache = SimpleSLRUCache(/*max_protected_size=*/5, /*max_total_size=*/10);
    slru_cache.set(1, std::make_shared<int>(2));
    slru_cache.set(1, std::make_shared<int>(3));
    auto val = slru_cache.get(1);
    ASSERT_TRUE(val != nullptr);
    ASSERT_TRUE(*val == 3);
}

TEST(SLRUCache, get)
{
    using SimpleSLRUCache = DB::SLRUCache<int, int>;
    auto slru_cache = SimpleSLRUCache(/*max_protected_size=*/5, /*max_total_size=*/10);
    slru_cache.set(1, std::make_shared<int>(2));
    slru_cache.set(2, std::make_shared<int>(3));
    SimpleSLRUCache::MappedPtr value = slru_cache.get(1);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 2);

    value = slru_cache.get(2);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 3);
}

struct ValueWeight
{
    size_t operator()(const size_t & x) const { return x; }
};

TEST(SLRUCache, evictOnWeight)
{
    using SimpleSLRUCache = DB::SLRUCache<int, size_t, std::hash<int>, ValueWeight>;
    auto slru_cache = SimpleSLRUCache(/*max_protected_size=*/5, /*max_total_size=*/10);
    slru_cache.set(1, std::make_shared<size_t>(2));
    slru_cache.set(2, std::make_shared<size_t>(3));
    slru_cache.set(3, std::make_shared<size_t>(4));
    slru_cache.set(3, std::make_shared<size_t>(5));

    auto n = slru_cache.count();
    ASSERT_EQ(n, 2);

    auto w = slru_cache.weight();
    ASSERT_EQ(w, 9);

    auto value = slru_cache.get(1);
    ASSERT_TRUE(value == nullptr);
    value = slru_cache.get(2);
    ASSERT_TRUE(value == nullptr);
}

TEST(SLRUCache, evictFromProtectedPart)
{
    using SimpleSLRUCache = DB::SLRUCache<int, size_t, std::hash<int>, ValueWeight>;
    auto slru_cache = SimpleSLRUCache(/*max_protected_size=*/5, /*max_total_size=*/10);
    slru_cache.set(1, std::make_shared<size_t>(2));
    slru_cache.set(1, std::make_shared<size_t>(2));

    slru_cache.set(2, std::make_shared<size_t>(5));
    slru_cache.set(2, std::make_shared<size_t>(5));
    
    slru_cache.set(3, std::make_shared<size_t>(5));

    auto value = slru_cache.get(1);
    ASSERT_TRUE(value == nullptr);
}

TEST(SLRUCache, evictStreamProtected)
{
    using SimpleSLRUCache = DB::SLRUCache<int, size_t, std::hash<int>, ValueWeight>;
    auto slru_cache = SimpleSLRUCache(/*max_protected_size=*/5, /*max_total_size=*/10);
    slru_cache.set(1, std::make_shared<size_t>(2));
    slru_cache.set(1, std::make_shared<size_t>(2));

    slru_cache.set(2, std::make_shared<size_t>(3));
    slru_cache.set(2, std::make_shared<size_t>(3));
    
    for (int key = 3; key < 10; ++key) {
        slru_cache.set(key, std::make_shared<size_t>(1 + key % 5));
    }

    auto value = slru_cache.get(1);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 2);

    value = slru_cache.get(2);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 3);
}

TEST(SLRUCache, getOrSet)
{
    using SimpleSLRUCache = DB::SLRUCache<int, size_t, std::hash<int>, ValueWeight>;
    auto slru_cache = SimpleSLRUCache(/*max_protected_size=*/5, /*max_total_size=*/10);
    size_t x = 5;
    auto load_func = [&] { return std::make_shared<size_t>(x); };
    auto [value, loaded] = slru_cache.getOrSet(1, load_func);
    ASSERT_TRUE(value != nullptr);
    ASSERT_TRUE(*value == 5);
}
