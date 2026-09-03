#include <iomanip>
#include <gtest/gtest.h>
#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>

/// Use MarkCache* for tests (to avoid introducing one more metric)
namespace CurrentMetrics
{
    extern const Metric MarkCacheBytes;
    extern const Metric MarkCacheFiles;
}

TEST(LRUCache, set)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto lru_cache = SimpleCacheBase("LRU", CurrentMetrics::MarkCacheBytes, CurrentMetrics::MarkCacheFiles, /*max_size_in_bytes*/ 10, /*max_count*/ 10, /*size_ratio*/ 0.5);
    lru_cache.set(1, std::make_shared<int>(2));
    lru_cache.set(2, std::make_shared<int>(3));

    ASSERT_EQ(lru_cache.sizeInBytes(), 2);
    ASSERT_EQ(lru_cache.count(), 2);

    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::MarkCacheBytes), 2);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::MarkCacheFiles), 2);
}

TEST(LRUCache, update)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto lru_cache = SimpleCacheBase("LRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes*/ 10, /*max_count*/ 10, /*size_ratio*/ 0.5);
    lru_cache.set(1, std::make_shared<int>(2));
    lru_cache.set(1, std::make_shared<int>(3));
    auto val = lru_cache.get(1);
    ASSERT_TRUE(val != nullptr);
    ASSERT_TRUE(*val == 3);
}

TEST(LRUCache, get)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto lru_cache = SimpleCacheBase("LRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes*/ 10, /*max_count*/ 10, /*size_ratio*/ 0.5);
    lru_cache.set(1, std::make_shared<int>(2));
    lru_cache.set(2, std::make_shared<int>(3));
    SimpleCacheBase::MappedPtr value = lru_cache.get(1);
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
    using SimpleCacheBase = DB::CacheBase<int, size_t>;
    auto lru_cache = SimpleCacheBase("LRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes*/ 20, /*max_count*/ 3, /*size_ratio*/ 0.5);
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
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;
    auto lru_cache = SimpleCacheBase("LRU", CurrentMetrics::MarkCacheBytes, CurrentMetrics::MarkCacheFiles, /*max_size_in_bytes*/ 10, /*max_count*/ 10, /*size_ratio*/ 0.5);
    lru_cache.set(1, std::make_shared<size_t>(2));
    lru_cache.set(2, std::make_shared<size_t>(3));
    lru_cache.set(3, std::make_shared<size_t>(4));
    lru_cache.set(4, std::make_shared<size_t>(5));

    auto n = lru_cache.count();
    ASSERT_EQ(n, 2);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::MarkCacheFiles), 2);

    auto w = lru_cache.sizeInBytes();
    ASSERT_EQ(w, 9);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::MarkCacheBytes), 9);

    auto value = lru_cache.get(1);
    ASSERT_TRUE(value == nullptr);
    value = lru_cache.get(2);
    ASSERT_TRUE(value == nullptr);
}

TEST(LRUCache, getOrSet)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;
    auto lru_cache = SimpleCacheBase("LRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes*/ 10, /*max_count*/ 10, /*size_ratio*/ 0.5);
    size_t x = 10;
    auto load_func = [&] { return std::make_shared<size_t>(x); };
    auto [value, loaded] = lru_cache.getOrSet(1, load_func);
    ASSERT_TRUE(value != nullptr);
    ASSERT_TRUE(*value == 10);
}


TEST(LRUCache, noOnRemoveEntryCallback)
{
    DB::LRUCachePolicy<std::string, size_t> lru_cache = {CurrentMetrics::end(), CurrentMetrics::end(), 10, 1, {}};
    lru_cache.set("key1", std::make_shared<size_t>(10));
    auto value = lru_cache.get("key1");
    ASSERT_TRUE(value != nullptr);
    ASSERT_TRUE(*value == 10);
    lru_cache.set("key2", std::make_shared<size_t>(20));
    value = lru_cache.get("key1");
    ASSERT_TRUE(value == nullptr);
    value = lru_cache.get("key2");
    ASSERT_TRUE(value != nullptr);
    ASSERT_TRUE(*value == 20);
}
