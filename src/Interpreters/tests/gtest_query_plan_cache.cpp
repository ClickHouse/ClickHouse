#include <Interpreters/Cache/QueryPlanCache.h>

#include <gtest/gtest.h>

#include <thread>
#include <vector>

namespace DB
{
namespace
{

QueryPlanCacheKey makeKey(UInt64 id, String ast_identity = {})
{
    QueryPlanCacheKey key;
    key.ast_hash.low64 = id;
    key.ast_hash.high64 = id + 1;
    key.ast_identity = ast_identity.empty() ? "SELECT " + std::to_string(id) : std::move(ast_identity);
    key.semantic_settings_hash = id + 2;
    return key;
}

QueryPlanCacheEntry makeEntry(size_t size)
{
    QueryPlanCacheEntry entry;
    entry.serialized_plan.assign(size, 'x');
    return entry;
}

}

TEST(QueryPlanCache, ExactASTIdentityRejectsHashCollision)
{
    QueryPlanCache cache(/*max_size_in_bytes=*/1000, /*max_entries=*/10);
    auto first = makeKey(1, "SELECT 1 AS value");
    auto collision = makeKey(1, "SELECT 2 AS value");

    cache.set(first, makeEntry(10));

    EXPECT_NE(cache.get(first), nullptr);
    EXPECT_EQ(cache.get(collision), nullptr);
    EXPECT_EQ(cache.count(), 1);
}

TEST(QueryPlanCache, EqualCanonicalASTProducesEqualKeyAndHasher)
{
    auto first = makeKey(1, "SELECT value FROM default.t");
    auto second = makeKey(1, "SELECT value FROM default.t");

    EXPECT_EQ(first, second);
    EXPECT_EQ(QueryPlanCacheKeyHasher{}(first), QueryPlanCacheKeyHasher{}(second));
}

TEST(QueryPlanCache, SameKeyReplacementKeepsSingleEntry)
{
    QueryPlanCache cache(/*max_size_in_bytes=*/100, /*max_entries=*/10);
    auto key = makeKey(1);

    cache.set(key, makeEntry(4));
    cache.set(key, makeEntry(6));

    auto entry = cache.get(key);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->serialized_plan.size(), 6);
    EXPECT_EQ(cache.count(), 1);
}

TEST(QueryPlanCache, ClearRemovesEntries)
{
    QueryPlanCache cache(/*max_size_in_bytes=*/100, /*max_entries=*/10);
    auto key = makeKey(1);
    cache.set(key, makeEntry(6));

    cache.clear();

    EXPECT_EQ(cache.get(key), nullptr);
    EXPECT_EQ(cache.count(), 0);
    EXPECT_EQ(cache.sizeInBytes(), 0);
}

TEST(QueryPlanCache, GlobalSizeLimitEvictsEntries)
{
    QueryPlanCache cache(/*max_size_in_bytes=*/50, /*max_entries=*/10);
    auto first = makeKey(1);
    auto second = makeKey(2);

    cache.set(first, makeEntry(30));
    cache.set(second, makeEntry(30));

    EXPECT_EQ(cache.get(first), nullptr);
    EXPECT_NE(cache.get(second), nullptr);
    EXPECT_EQ(cache.count(), 1);
}

TEST(QueryPlanCache, ZeroMaxEntriesDisablesZeroWeightInsert)
{
    QueryPlanCache cache(/*max_size_in_bytes=*/100, /*max_entries=*/0);
    auto key = makeKey(1);

    cache.set(key, makeEntry(0));

    EXPECT_EQ(cache.get(key), nullptr);
    EXPECT_EQ(cache.count(), 0);
}

TEST(QueryPlanCache, ConcurrentSameKeyInsertKeepsSingleEntry)
{
    QueryPlanCache cache(/*max_size_in_bytes=*/10000, /*max_entries=*/100);
    auto key = makeKey(1);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < 8; ++i)
        threads.emplace_back([&, i] { cache.set(key, makeEntry(10 + i)); });
    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(cache.count(), 1);
    EXPECT_NE(cache.get(key), nullptr);
}
}
