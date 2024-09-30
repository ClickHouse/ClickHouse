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

TEST(SIEVECache, ComplexEvictTest)
{
    using SimpleCacheBase = DB::CacheBase<int, size_t, std::hash<int>, ValueWeight>;
    auto sieve_cache = SimpleCacheBase("SIEVE", /*max_size_in_bytes*/ 20, /*max_count*/ 7, /*size_ratio*/ 0.5);

    // Adding elements to the cache
    sieve_cache.set(1, std::make_shared<size_t>(2)); // visited = 0
    sieve_cache.set(2, std::make_shared<size_t>(3)); // visited = 0
    sieve_cache.set(3, std::make_shared<size_t>(4)); // visited = 0
    sieve_cache.set(4, std::make_shared<size_t>(5)); // visited = 0
    sieve_cache.set(5, std::make_shared<size_t>(1)); // visited = 0
    sieve_cache.set(6, std::make_shared<size_t>(1)); // visited = 0
    sieve_cache.set(7, std::make_shared<size_t>(1)); // visited = 0

    // Manually setting visited flag
    sieve_cache.get(1); // visited = 1
    sieve_cache.get(2); // visited = 1
    sieve_cache.get(3); // visited = 1
    sieve_cache.get(5); // visited = 1
    sieve_cache.get(7); // visited = 1

    // Expected visited flags: 1 1 1 0 1 0 1
    // After removeOverflow: 0 0 0 1 1 0 1
    sieve_cache.set(8, std::make_shared<size_t>(6)); // This should trigger eviction

    auto n = sieve_cache.count();
    ASSERT_EQ(n, 6);

    const auto & cache_policy = sieve_cache.getCachePolicy();
    const auto & sieve_cache_policy = dynamic_cast<const DB::SIEVECachePolicy<int, size_t> &>(cache_policy);
    ASSERT_TRUE(sieve_cache_policy.isVisited(1) == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(2) == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(3) == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(5) == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(6) == 0);
    ASSERT_TRUE(sieve_cache_policy.isVisited(7) == 1);
    ASSERT_TRUE(sieve_cache_policy.isVisited(8) == 1);
}
