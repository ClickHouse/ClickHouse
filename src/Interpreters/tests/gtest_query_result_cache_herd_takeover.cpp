#include <Interpreters/Cache/QueryResultCache.h>

#include <base/unit.h>

#include <gtest/gtest.h>

using namespace DB;

/// A query that finished waiting for the herd executor and still found the cache empty (the executor died, was
/// cancelled, or the entry was cleared) executes the result itself. It then has to take over as the executor via
/// `tryBecomeHerdExecutor`, otherwise it would own no token and a query arriving in the meantime would install a
/// token of its own, whose waiters block on it even though the takeover execution repopulates the cache first.

namespace
{

const IASTHash ast_hash{123456789, 987654321};
const UUID user = UUID(UInt128(1));

auto neverCancelled()
{
    return []() { return false; };
}

QueryResultCache::CoalescingKey makeKey()
{
    return QueryResultCache::CoalescingKey{ast_hash, user, {}, /*share_between_users=*/ false};
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

    auto executor_token = cache.tryBecomeHerdExecutor(key, "executor");
    ASSERT_TRUE(executor_token);

    /// Somebody else is the executor and wakes its own waiters - no takeover.
    EXPECT_FALSE(cache.tryBecomeHerdExecutor(key, "other"));

    cache.releaseHerdToken(key, executor_token);

    /// The key is free again.
    auto takeover_token = cache.tryBecomeHerdExecutor(key, "other");
    ASSERT_TRUE(takeover_token);
    EXPECT_NE(takeover_token, executor_token);
    cache.releaseHerdToken(key, takeover_token);
}

TEST(QueryResultCacheHerdTakeover, SucceedsAfterATimedOutWaiterDroppedTheStaleToken)
{
    QueryResultCache cache = makeCache();
    const auto key = makeKey();

    /// The executor is presumed dead: it never calls `releaseHerdToken`.
    ASSERT_TRUE(cache.acquireOrWaitHerdToken(key, std::chrono::milliseconds(10000), "dead_executor", neverCancelled()));

    /// The waiter times out and drops the stale token.
    EXPECT_FALSE(cache.acquireOrWaitHerdToken(key, std::chrono::milliseconds(1), "waiter", neverCancelled()));

    /// That waiter now executes the query itself, so it must become the new executor.
    auto takeover_token = cache.tryBecomeHerdExecutor(key, "waiter");
    ASSERT_TRUE(takeover_token);
    cache.releaseHerdToken(key, takeover_token);
}

TEST(QueryResultCacheHerdTakeover, LaterQueriesCoalesceOnTheTakeoverToken)
{
    QueryResultCache cache = makeCache();
    const auto key = makeKey();

    auto takeover_token = cache.tryBecomeHerdExecutor(key, "taker");
    ASSERT_TRUE(takeover_token);

    /// A query arriving while the takeover execution runs must coalesce on it instead of becoming an executor of its
    /// own: `acquireOrWaitHerdToken` returns nullptr (it waited - here with a minimal timeout to keep the test instant)
    /// and a competing takeover attempt fails, i.e. the key is owned.
    EXPECT_FALSE(cache.tryBecomeHerdExecutor(key, "later"));
    EXPECT_FALSE(cache.acquireOrWaitHerdToken(key, std::chrono::milliseconds(1), "later", neverCancelled()));

    cache.releaseHerdToken(key, takeover_token);
}

TEST(QueryResultCacheHerdTakeover, AQueryNeverWaitsForItsOwnToken)
{
    /// The Planner-level subquery path takes a token while planning and releases it only after the subquery ran, so a
    /// query containing the same subquery twice reaches `acquireOrWaitHerdToken` for the second occurrence while still
    /// owning the token of the first one. Waiting would block the query on its own unfinished computation until the timeout.
    QueryResultCache cache = makeCache();
    const auto key = makeKey();

    auto own_token = cache.acquireOrWaitHerdToken(key, std::chrono::milliseconds(10000), "query_1", neverCancelled());
    ASSERT_TRUE(own_token);

    /// Same query id: returns immediately instead of waiting for the (long) timeout.
    const auto before = std::chrono::steady_clock::now();
    EXPECT_FALSE(cache.acquireOrWaitHerdToken(key, std::chrono::milliseconds(10000), "query_1", neverCancelled()));
    EXPECT_LT(std::chrono::steady_clock::now() - before, std::chrono::seconds(5));

    /// Another query is still a regular waiter and does not steal the key.
    EXPECT_FALSE(cache.tryBecomeHerdExecutor(key, "query_2"));

    cache.releaseHerdToken(key, own_token);
}
