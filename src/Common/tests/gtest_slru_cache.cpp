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

TEST(SLRUCache, set)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto slru_cache = SimpleCacheBase("SLRU", CurrentMetrics::MarkCacheBytes, CurrentMetrics::MarkCacheFiles, /*max_size_in_bytes=*/10, /*max_count=*/0, /*size_ratio*/0.5);
    slru_cache.set(1, std::make_shared<int>(2));
    slru_cache.set(2, std::make_shared<int>(3));

    ASSERT_EQ(slru_cache.sizeInBytes(), 2);
    ASSERT_EQ(slru_cache.count(), 2);

    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::MarkCacheBytes), 2);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::MarkCacheFiles), 2);
}

TEST(SLRUCache, update)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto slru_cache = SimpleCacheBase("SLRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes=*/10, /*max_count=*/0, /*size_ratio*/0.5);
    slru_cache.set(1, std::make_shared<int>(2));
    slru_cache.set(1, std::make_shared<int>(3));

    auto value = slru_cache.get(1);
    ASSERT_TRUE(value != nullptr);
    ASSERT_TRUE(*value == 3);
}

TEST(SLRUCache, get)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto slru_cache = SimpleCacheBase("SLRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes=*/10, /*max_count=*/0, /*size_ratio*/0.5);
    slru_cache.set(1, std::make_shared<int>(2));
    slru_cache.set(2, std::make_shared<int>(3));

    auto value = slru_cache.get(1);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 2);

    value = slru_cache.get(2);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 3);
}

TEST(SLRUCache, remove)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto slru_cache = SimpleCacheBase("SLRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes=*/10, /*max_count=*/0, /*size_ratio*/0.5);
    slru_cache.set(1, std::make_shared<int>(2));
    slru_cache.set(2, std::make_shared<int>(3));

    auto value = slru_cache.get(1);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 2);

    slru_cache.remove(2);
    value = slru_cache.get(2);
    ASSERT_TRUE(value == nullptr);
}

TEST(SLRUCache, removeFromProtected)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto slru_cache = SimpleCacheBase("SLRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes=*/2, /*max_count=*/0, /*size_ratio*/0.5);
    slru_cache.set(1, std::make_shared<int>(2));
    slru_cache.set(1, std::make_shared<int>(3));

    auto value = slru_cache.get(1);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 3);

    slru_cache.remove(1);
    value = slru_cache.get(1);
    ASSERT_TRUE(value == nullptr);

    slru_cache.set(1, std::make_shared<int>(4));
    slru_cache.set(1, std::make_shared<int>(5));

    slru_cache.set(2, std::make_shared<int>(6));
    slru_cache.set(3, std::make_shared<int>(7));

    value = slru_cache.get(1);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 5);

    value = slru_cache.get(3);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 7);

    value = slru_cache.get(2);
    ASSERT_TRUE(value == nullptr);
}

TEST(SLRUCache, clear)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto slru_cache = SimpleCacheBase("SLRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes=*/10, /*max_count=*/0, /*size_ratio*/0.5);
    slru_cache.set(1, std::make_shared<int>(2));
    slru_cache.set(2, std::make_shared<int>(3));

    slru_cache.set(2, std::make_shared<int>(4)); /// add to protected_queue

    slru_cache.clear();

    auto value = slru_cache.get(1);
    ASSERT_TRUE(value == nullptr);

    value = slru_cache.get(2);
    ASSERT_TRUE(value == nullptr);
}

struct ValueWeight
{
    size_t operator()(const size_t & x) const { return x; }
};

TEST(SLRUCache, evictOnElements)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;
    auto slru_cache = SimpleCacheBase(CurrentMetrics::MarkCacheBytes, CurrentMetrics::MarkCacheFiles, /*max_size_in_bytes=*/10, /*max_count=*/1, /*size_ratio*/0.5);
    slru_cache.set(1, std::make_shared<size_t>(2));
    slru_cache.set(2, std::make_shared<size_t>(3));

    ASSERT_EQ(slru_cache.count(), 1);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::MarkCacheFiles), 1);
    ASSERT_EQ(slru_cache.sizeInBytes(), 3);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::MarkCacheBytes), 3);

    auto value = slru_cache.get(1);
    ASSERT_TRUE(value == nullptr);
    value = slru_cache.get(2);
    ASSERT_TRUE(value != nullptr);
    ASSERT_TRUE(*value == 3);
}


TEST(SLRUCache, evictOnWeight)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;
    auto slru_cache = SimpleCacheBase(CurrentMetrics::MarkCacheBytes, CurrentMetrics::MarkCacheFiles, /*max_size_in_bytes=*/10, /*max_count=*/0, /*size_ratio*/0.5);
    slru_cache.set(1, std::make_shared<size_t>(2));
    slru_cache.set(2, std::make_shared<size_t>(3));
    slru_cache.set(3, std::make_shared<size_t>(4));
    slru_cache.set(4, std::make_shared<size_t>(5));

    ASSERT_EQ(slru_cache.count(), 2);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::MarkCacheFiles), 2);
    ASSERT_EQ(slru_cache.sizeInBytes(), 9);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::MarkCacheBytes), 9);

    auto value = slru_cache.get(1);
    ASSERT_TRUE(value == nullptr);
    value = slru_cache.get(2);
    ASSERT_TRUE(value == nullptr);
}

TEST(SLRUCache, evictFromProtectedPart)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;
    auto slru_cache = SimpleCacheBase("SLRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes=*/10, /*max_count=*/0, /*size_ratio*/0.5);
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
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;
    auto slru_cache = SimpleCacheBase("SLRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes=*/10, /*max_count=*/0, /*size_ratio*/0.5);
    slru_cache.set(1, std::make_shared<size_t>(2));
    slru_cache.set(1, std::make_shared<size_t>(2));

    slru_cache.set(2, std::make_shared<size_t>(3));
    slru_cache.set(2, std::make_shared<size_t>(3));

    for (int key = 3; key < 10; ++key)
    {
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
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;
    auto slru_cache = SimpleCacheBase("SLRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes=*/10, /*max_count=*/0, /*size_ratio*/0.5);
    size_t x = 5;
    auto load_func = [&] { return std::make_shared<size_t>(x); };
    auto [value, loaded] = slru_cache.getOrSet(1, load_func);
    ASSERT_TRUE(value != nullptr);
    ASSERT_TRUE(*value == 5);
}

TEST(SLRUCache, MaxCount)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;

    size_t x = 5;
    auto load_func = [&] { return std::make_shared<size_t>(x); };

    for (size_t max_count = 1; max_count < 1024; max_count *= 2)
    {
        SimpleCacheBase slru_cache("SLRU", CurrentMetrics::end(), CurrentMetrics::end(),
                                   /*max_size_in_bytes=*/1'000'000'000,
                                   /*max_count=*/max_count,
                                   /*size_ratio*/0.5);
        for (size_t i = 0; i < 10; ++i)
        {
            auto [value, loaded] = slru_cache.getOrSet(i, load_func);
            ASSERT_NE(value, nullptr)
                << "max_count = " << max_count << ", i = " << i;
            ASSERT_EQ(*value, 5)
                << "max_count = " << max_count << ", i = " << i;
            ASSERT_EQ(slru_cache.count(), std::min(i + 1, max_count))
                << "max_count = " << max_count << ", i = " << i;
        }
    }
}

TEST(SLRUCache, noOnRemoveEntryCallback)
{
    DB::SLRUCachePolicy<std::string, size_t> slru_cache = {CurrentMetrics::end(), CurrentMetrics::end(), 20, 1, 0.5, {}};
    slru_cache.set("key1", std::make_shared<size_t>(10));
    slru_cache.set("key2", std::make_shared<size_t>(20));
    auto value = slru_cache.get("key2");
    ASSERT_TRUE(value != nullptr);
    value = slru_cache.get("key1");
    ASSERT_TRUE(value == nullptr);
}
