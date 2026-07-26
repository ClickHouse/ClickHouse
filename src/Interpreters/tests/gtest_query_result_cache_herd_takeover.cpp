#include <Interpreters/Cache/QueryResultCache.h>

#include <base/unit.h>

#include <gtest/gtest.h>

using namespace DB;

/// A query that finished waiting for the herd executor and still found the cache empty (the executor died, was
/// cancelled, or the entry was cleared) executes the result itself. It then has to take over as the executor via
/// `tryTakeOverAsyncInsert`, otherwise it would own no token and a query arriving in the meantime would install a
/// token of its own, whose waiters block on it even though the takeover execution repopulates the cache first.

namespace
{

const IASTHash ast_hash{123456789, 987654321};
const UUID user = UUID(UInt128(1));

QueryResultCache::HerdCoalescingKey makeKey()
{
    return QueryResultCache::HerdCoalescingKey{ast_hash, user, {}, /*share_between_users=*/ false, ""};
}

QueryResultCache makeCache()
{
    return QueryResultCache(/*max_size_in_bytes=*/ 1_MiB, /*max_entries=*/ 16, /*max_entry_size_in_bytes_=*/ 1_MiB, /*max_entry_size_in_rows_=*/ 1000);
}

}

TEST(QueryResultCacheHerdTakeover, FailsWhileAnExecutorOwnsTheKey)
{
    QueryResultCache cache = makeCache();
    const auto key = makeKey();

    auto executor_token = cache.startAsyncInsert(key, std::chrono::milliseconds(10000));
    ASSERT_TRUE(executor_token);

    /// Somebody else is the executor and wakes its own waiters - no takeover.
    EXPECT_FALSE(cache.tryTakeOverAsyncInsert(key));

    cache.finishAsyncInsert(executor_token);

    /// The key is free again.
    auto takeover_token = cache.tryTakeOverAsyncInsert(key);
    ASSERT_TRUE(takeover_token);
    EXPECT_NE(takeover_token, executor_token);
    cache.finishAsyncInsert(takeover_token);
}

TEST(QueryResultCacheHerdTakeover, SucceedsAfterATimedOutWaiterDroppedTheStaleToken)
{
    QueryResultCache cache = makeCache();
    const auto key = makeKey();

    /// The executor is presumed dead: it never calls `finishAsyncInsert`.
    ASSERT_TRUE(cache.startAsyncInsert(key, std::chrono::milliseconds(10000)));

    /// The waiter times out and drops the stale token.
    EXPECT_FALSE(cache.startAsyncInsert(key, std::chrono::milliseconds(1)));

    /// That waiter now executes the query itself, so it must become the new executor.
    auto takeover_token = cache.tryTakeOverAsyncInsert(key);
    ASSERT_TRUE(takeover_token);
    cache.finishAsyncInsert(takeover_token);
}

TEST(QueryResultCacheHerdTakeover, LaterQueriesCoalesceOnTheTakeoverToken)
{
    QueryResultCache cache = makeCache();
    const auto key = makeKey();

    auto takeover_token = cache.tryTakeOverAsyncInsert(key);
    ASSERT_TRUE(takeover_token);

    /// A query arriving while the takeover execution runs must coalesce on it instead of becoming an executor of its
    /// own: `startAsyncInsert` returns nullptr (it waited - here with a minimal timeout to keep the test instant) and
    /// a competing takeover attempt fails, i.e. the key is owned.
    EXPECT_FALSE(cache.tryTakeOverAsyncInsert(key));
    EXPECT_FALSE(cache.startAsyncInsert(key, std::chrono::milliseconds(1)));

    cache.finishAsyncInsert(takeover_token);
}
