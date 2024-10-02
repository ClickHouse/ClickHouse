#include <iomanip>
#include <gtest/gtest.h>
#include <Common/CacheBase.h>

TEST(SIEVECache, set)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto sieve_cache = SimpleCacheBase("SIEVE", /*max_size_in_bytes*/ 10, /*max_count*/ 10, /*size_ratio*/ 0.5);
    sieve_cache.set(1, std::make_shared<int>(2));
    sieve_cache.set(2, std::make_shared<int>(3));

    auto w = sieve_cache.sizeInBytes();
    auto n = sieve_cache.count();
    ASSERT_EQ(w, 2);
    ASSERT_EQ(n, 2);
}

TEST(SIEVECache, update)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto sieve_cache = SimpleCacheBase("SIEVE", /*max_size_in_bytes*/ 10, /*max_count*/ 10, /*size_ratio*/ 0.5);
    sieve_cache.set(1, std::make_shared<int>(2));
    sieve_cache.set(1, std::make_shared<int>(3));
    auto val = sieve_cache.get(1);
    ASSERT_TRUE(val != nullptr);
    ASSERT_TRUE(*val == 3);
}

TEST(SIEVECache, get)
{
    using SimpleCacheBase = DB::CacheBase<int, int>;
    auto sieve_cache = SimpleCacheBase("SIEVE", /*max_size_in_bytes*/ 10, /*max_count*/ 10, /*size_ratio*/ 0.5);
    sieve_cache.set(1, std::make_shared<int>(2));
    sieve_cache.set(2, std::make_shared<int>(3));
    SimpleCacheBase::MappedPtr value = sieve_cache.get(1);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 2);

    value = sieve_cache.get(2);
    ASSERT_TRUE(value != nullptr);
    ASSERT_EQ(*value, 3);
}

struct ValueWeight
{
    size_t operator()(const size_t & x) const { return x; }
};

TEST(SIEVECache, evictOnSize)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t>;
    auto sieve_cache = SimpleCacheBase("SIEVE", /*max_size_in_bytes*/ 20, /*max_count*/ 3, /*size_ratio*/ 0.5);
    sieve_cache.set(1, std::make_shared<size_t>(2));
    sieve_cache.set(2, std::make_shared<size_t>(3));
    sieve_cache.set(3, std::make_shared<size_t>(4));
    sieve_cache.set(4, std::make_shared<size_t>(5));

    auto n = sieve_cache.count();
    ASSERT_EQ(n, 3);

    auto value = sieve_cache.get(1);
    ASSERT_TRUE(value == nullptr);
}

TEST(SIEVECache, evictOnWeight)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;
    auto sieve_cache = SimpleCacheBase("SIEVE", /*max_size_in_bytes*/ 10, /*max_count*/ 10, /*size_ratio*/ 0.5);
    sieve_cache.set(1, std::make_shared<size_t>(2));
    sieve_cache.set(2, std::make_shared<size_t>(3));
    sieve_cache.set(3, std::make_shared<size_t>(4));
    sieve_cache.set(4, std::make_shared<size_t>(5));

    auto n = sieve_cache.count();
    ASSERT_EQ(n, 2);

    auto w = sieve_cache.sizeInBytes();
    ASSERT_EQ(w, 9);

    auto value = sieve_cache.get(1);
    ASSERT_TRUE(value == nullptr);
    value = sieve_cache.get(2);
    ASSERT_TRUE(value == nullptr);
    value = sieve_cache.get(3);
    ASSERT_TRUE(value != nullptr);
    value = sieve_cache.get(4);
    ASSERT_TRUE(value != nullptr);
}

TEST(SIEVECache, getOrSet)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;
    auto sieve_cache = SimpleCacheBase("SIEVE", /*max_size_in_bytes*/ 10, /*max_count*/ 10, /*size_ratio*/ 0.5);
    size_t x = 10;
    auto load_func = [&] { return std::make_shared<size_t>(x); };
    auto [value, loaded] = sieve_cache.getOrSet(1, load_func);
    ASSERT_TRUE(value != nullptr);
    ASSERT_TRUE(*value == 10);
}

void assertQueue(const std::vector<int> & queue, const std::vector<int> expected_keys)
{
    ASSERT_EQ(queue.size(), expected_keys.size());
    std::cerr << "Queue size: " + std::to_string(queue.size()) + "\n";
    for (size_t i = 0; i < queue.size(); ++i)
    {
        const auto & key = queue[i];
        const auto & expected = expected_keys[i];
        std::cerr << fmt::format("i: {}, key: {}, expected: {}\n", i, key, expected);
        ASSERT_EQ(key, expected);
    }
}

TEST(SIEVECache, ComplexEvictTest)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;
    auto sieve_cache = SimpleCacheBase("SIEVE", /*max_size_in_bytes*/ 20, /*max_count*/ 7, /*size_ratio*/ 0.5);

    // Adding elements to the cache
    sieve_cache.set(1, std::make_shared<size_t>(2)); // visited = 0
    sieve_cache.set(2, std::make_shared<size_t>(2)); // visited = 0
    sieve_cache.set(3, std::make_shared<size_t>(2)); // visited = 0
    sieve_cache.set(4, std::make_shared<size_t>(2)); // visited = 0
    sieve_cache.set(5, std::make_shared<size_t>(2)); // visited = 0
    sieve_cache.set(6, std::make_shared<size_t>(2)); // visited = 0
    sieve_cache.set(7, std::make_shared<size_t>(2)); // visited = 0

    const auto & cache_policy = sieve_cache.getCachePolicy();
    const auto & sieve_cache_policy = dynamic_cast<const DB::SIEVECachePolicy<int, size_t, std::hash<int>, ValueWeight> &>(cache_policy);

    ASSERT_TRUE(sieve_cache_policy.isVisited(1).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(2).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(3).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(4).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(5).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(6).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(7).value() == 0);

    assertQueue(sieve_cache_policy.dumpQueue(), {1, 2, 3, 4, 5, 6, 7});
    ASSERT_EQ(sieve_cache_policy.getHand().value(), 1);

    sieve_cache.get(1);
    sieve_cache.get(3);
    sieve_cache.get(5);
    sieve_cache.get(7);

    assertQueue(sieve_cache_policy.dumpQueue(), {1, 2, 3, 4, 5, 6, 7});
    ASSERT_EQ(sieve_cache_policy.getHand().value(), 1);

    ASSERT_TRUE(sieve_cache_policy.isVisited(1).value() == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(2).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(3).value() == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(4).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(5).value() == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(6).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(7).value() == 1);

    ASSERT_EQ(sieve_cache.count(), 7);

    sieve_cache.set(8, std::make_shared<size_t>(2));

    ASSERT_EQ(sieve_cache.count(), 7);

    assertQueue(sieve_cache_policy.dumpQueue(), {1, 3, 4, 5, 6, 7, 8});
    ASSERT_EQ(sieve_cache_policy.getHand().value(), 3);

    ASSERT_TRUE(sieve_cache_policy.isVisited(1).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(3).value() == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(4).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(5).value() == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(6).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(7).value() == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(8).value() == 0);

    sieve_cache.set(9, std::make_shared<size_t>(2));

    ASSERT_EQ(sieve_cache.count(), 7);

    assertQueue(sieve_cache_policy.dumpQueue(), {1, 3, 5, 6, 7, 8, 9});
    ASSERT_EQ(sieve_cache_policy.getHand().value(), 5);

    ASSERT_TRUE(sieve_cache_policy.isVisited(1).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(3).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(5).value() == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(6).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(7).value() == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(8).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(9).value() == 0);

    sieve_cache.set(10, std::make_shared<size_t>(2));

    ASSERT_EQ(sieve_cache.count(), 7);

    assertQueue(sieve_cache_policy.dumpQueue(), {1, 3, 5, 7, 8, 9, 10});
    ASSERT_EQ(sieve_cache_policy.getHand().value(), 7);

    ASSERT_TRUE(sieve_cache_policy.isVisited(1).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(3).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(5).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(7).value() == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(8).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(9).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(10).value() == 0);

    sieve_cache.set(11, std::make_shared<size_t>(12));

    ASSERT_EQ(sieve_cache.count(), 5);

    assertQueue(sieve_cache_policy.dumpQueue(), {1, 3, 5, 7, 11});
    ASSERT_EQ(sieve_cache_policy.getHand().value(), 11);

    ASSERT_TRUE(sieve_cache_policy.isVisited(1).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(3).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(5).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(7).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(11).value() == 0);

    sieve_cache.set(12, std::make_shared<size_t>(4));

    ASSERT_EQ(sieve_cache.count(), 5);

    assertQueue(sieve_cache_policy.dumpQueue(), {1, 3, 5, 7, 12});
    ASSERT_EQ(sieve_cache_policy.getHand().value(), 12);

    ASSERT_TRUE(sieve_cache_policy.isVisited(1).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(3).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(5).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(7).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(12).value() == 0);

    sieve_cache.set(13, std::make_shared<size_t>(14));

    ASSERT_EQ(sieve_cache.count(), 4);

    assertQueue(sieve_cache_policy.dumpQueue(), {3, 5, 7, 13});

    ASSERT_TRUE(sieve_cache_policy.isVisited(3).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(5).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(7).value() == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(13).value() == 0);
}
