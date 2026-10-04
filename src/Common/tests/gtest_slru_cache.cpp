#include <iomanip>
#include <limits>
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
        for (int i = 0; i < 10; ++i)
        {
            auto [value, loaded] = slru_cache.getOrSet(i, load_func);
            ASSERT_NE(value, nullptr)
                << "max_count = " << max_count << ", i = " << i;
            ASSERT_EQ(*value, 5)
                << "max_count = " << max_count << ", i = " << i;
            ASSERT_EQ(slru_cache.count(), std::min(static_cast<size_t>(i + 1), max_count))
                << "max_count = " << max_count << ", i = " << i;
        }
    }
}

TEST(SLRUCache, MaxCountDoesNotStarveProbationary)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;

    size_t x = 5;
    auto load_func = [&] { return std::make_shared<size_t>(x); };

    /// Large enough that the byte limbs never bind, so only the count limbs are exercised.
    static constexpr size_t max_size_in_bytes = 1'000'000'000;

    /// A ratio of 1.0 hands the whole cache to the protected queue, so nothing can stay
    /// probationary. That is a misconfiguration rather than a case to protect against.
    for (double size_ratio : {0.0, 0.25, 0.5})
    {
        for (size_t max_count = 1; max_count <= 8; max_count *= 2)
        {
            SimpleCacheBase slru_cache("SLRU", CurrentMetrics::end(), CurrentMetrics::end(),
                                       max_size_in_bytes, max_count, size_ratio);

            /// A second access promotes an entry into the protected queue.
            for (size_t i = 0; i < max_count; ++i)
            {
                slru_cache.getOrSet(static_cast<int>(i), load_func);
                slru_cache.getOrSet(static_cast<int>(i), load_func);
            }

            slru_cache.getOrSet(100, load_func);
            EXPECT_NE(slru_cache.get(100), nullptr)
                << "max_count = " << max_count << ", size_ratio = " << size_ratio;
        }
    }

    /// setMaxCount is the second entry point into the protected bound: on shrink it walks the
    /// protected queue down to the new limit.
    {
        SimpleCacheBase slru_cache("SLRU", CurrentMetrics::end(), CurrentMetrics::end(),
                                   max_size_in_bytes, /*max_count=*/8, /*size_ratio*/0.5);
        for (size_t i = 0; i < 8; ++i)
        {
            slru_cache.getOrSet(static_cast<int>(i), load_func);
            slru_cache.getOrSet(static_cast<int>(i), load_func);
        }

        slru_cache.setMaxCount(4);

        slru_cache.getOrSet(100, load_func);
        EXPECT_NE(slru_cache.get(100), nullptr) << "after setMaxCount(4)";
    }

    /// max_entries is an unclamped server setting, so the protected bound must stay computable at
    /// the extremes of size_t.
    for (size_t max_count : {std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max() - 1024})
    {
        SimpleCacheBase slru_cache("SLRU", CurrentMetrics::end(), CurrentMetrics::end(),
                                   max_size_in_bytes, max_count, /*size_ratio*/1.0);
        slru_cache.getOrSet(7, load_func);
        EXPECT_NE(slru_cache.get(7), nullptr) << "max_count = " << max_count;
    }
}

TEST(SLRUCache, MaxCountProtectedBoundHonoursSizeRatio)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;

    size_t x = 5;
    auto load_func = [&] { return std::make_shared<size_t>(x); };

    static constexpr size_t max_size_in_bytes = 1'000'000'000;

    /// The protected bound is size_ratio * max_count, and exactly that many promoted entries
    /// survive a scan of fresh single-use keys. contains() is used to count them because get()
    /// would promote and reorder, changing the set being measured.
    auto count_scan_survivors = [&](size_t max_count, double size_ratio)
    {
        SimpleCacheBase slru_cache("SLRU", CurrentMetrics::end(), CurrentMetrics::end(),
                                   max_size_in_bytes, max_count, size_ratio);

        for (size_t i = 0; i < max_count; ++i)
        {
            slru_cache.getOrSet(static_cast<int>(i), load_func);
            slru_cache.getOrSet(static_cast<int>(i), load_func);
        }

        for (size_t j = 0; j < 2 * max_count; ++j)
            slru_cache.getOrSet(static_cast<int>(1000 + j), load_func);

        size_t survivors = 0;
        for (size_t i = 0; i < max_count; ++i)
            survivors += slru_cache.contains(static_cast<int>(i)) ? 1 : 0;
        return survivors;
    };

    EXPECT_EQ(count_scan_survivors(4, 0.5), 2u);
    EXPECT_EQ(count_scan_survivors(8, 0.5), 4u);
    EXPECT_EQ(count_scan_survivors(8, 0.25), 2u);
    EXPECT_EQ(count_scan_survivors(4, 0.25), 1u);

    /// A ratio of 1.0 protects the whole cache, so a scan evicts nothing.
    EXPECT_EQ(count_scan_survivors(4, 1.0), 4u);
    EXPECT_EQ(count_scan_survivors(8, 1.0), 8u);
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
