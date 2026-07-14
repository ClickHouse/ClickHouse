#include <Interpreters/Cache/QueryPlanCache.h>

#include <Core/UUID.h>
#include <gtest/gtest.h>

namespace DB
{
namespace
{

QueryPlanCacheKey makeKey(UInt64 id)
{
    QueryPlanCacheKey key;
    key.ast_hash.low64 = id;
    key.ast_hash.high64 = id + 1;
    key.semantic_settings_hash = id + 2;
    key.user_id = UUID(id + 3);
    return key;
}

QueryPlanCacheEntry makeEntry(size_t size)
{
    QueryPlanCacheEntry entry;
    entry.serialized_plan.assign(size, 'x');
    return entry;
}

}

TEST(QueryPlanCache, RejectsAndRemovesStaleFormatVersionEntry)
{
    QueryPlanCache cache(/*max_size_in_bytes=*/100, /*max_entries=*/10);
    auto key = makeKey(1);
    auto other_key = makeKey(2);
    other_key.user_id = key.user_id;

    auto stale_entry = makeEntry(10);
    stale_entry.format_version = QUERY_PLAN_CACHE_FORMAT_VERSION - 1;
    cache.set(key, std::move(stale_entry), /*max_size_in_bytes_for_user=*/10);

    EXPECT_EQ(cache.get(key), nullptr);
    EXPECT_EQ(cache.count(), 0);

    /// Removing the stale entry must also release its per-user quota charge.
    cache.set(other_key, makeEntry(10), /*max_size_in_bytes_for_user=*/10);

    EXPECT_NE(cache.get(other_key), nullptr);
    EXPECT_EQ(cache.count(), 1);
}

TEST(QueryPlanCache, SameKeyReplacementUpdatesUserQuotaAccounting)
{
    QueryPlanCache cache(/*max_size_in_bytes=*/100, /*max_entries=*/10);
    auto key = makeKey(1);
    auto other_key = makeKey(2);
    other_key.user_id = key.user_id;

    cache.set(key, makeEntry(4), /*max_size_in_bytes_for_user=*/10);
    cache.set(key, makeEntry(6), /*max_size_in_bytes_for_user=*/10);
    cache.set(other_key, makeEntry(4), /*max_size_in_bytes_for_user=*/10);

    EXPECT_NE(cache.get(key), nullptr);
    EXPECT_NE(cache.get(other_key), nullptr);
    EXPECT_EQ(cache.count(), 2);
}

TEST(QueryPlanCache, ClearDropsUserQuotaAccounting)
{
    QueryPlanCache cache(/*max_size_in_bytes=*/100, /*max_entries=*/10);
    auto key = makeKey(1);
    auto other_key = makeKey(2);
    other_key.user_id = key.user_id;

    cache.set(key, makeEntry(6), /*max_size_in_bytes_for_user=*/10);
    cache.clear();

    cache.set(other_key, makeEntry(10), /*max_size_in_bytes_for_user=*/10);

    EXPECT_EQ(cache.get(key), nullptr);
    EXPECT_NE(cache.get(other_key), nullptr);
    EXPECT_EQ(cache.count(), 1);
}

}
