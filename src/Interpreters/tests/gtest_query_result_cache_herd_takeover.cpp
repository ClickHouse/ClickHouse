#include <Interpreters/Cache/QueryResultCache.h>

#include <Common/tests/gtest_global_context.h>

#include <base/unit.h>

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

using namespace DB;

/// The herd token protocol of the query result cache: a query whose result is not in the cache either becomes the
/// "executor" of a group of concurrent, identical queries, or waits while the executor computes the result.

namespace
{

const IASTHash ast_hash{123456789, 987654321};
const IASTHash other_ast_hash{111111111, 222222222};
const UUID user = UUID(UInt128(1));

using Outcome = QueryResultCache::HerdWaitOutcome;

constexpr auto long_timeout = std::chrono::milliseconds(60000);

auto neverCancelled()
{
    return []() { return false; };
}

QueryResultCache::CoalescingKey makeKey(const IASTHash & hash = ast_hash, bool is_subquery = false)
{
    return QueryResultCache::CoalescingKey{hash, user, {}, /*share_between_users=*/ false, is_subquery};
}

QueryResultCache makeCache()
{
    return QueryResultCache(/*max_size_in_bytes=*/ 1_MiB, /*max_entries=*/ 16, /*max_entry_size_in_bytes_=*/ 1_MiB, /*max_entry_size_in_rows_=*/ 1000);
}

QueryResultCachePtr makeSharedCache()
{
    return std::make_shared<QueryResultCache>(/*max_size_in_bytes=*/ 1_MiB, /*max_entries=*/ 16, /*max_entry_size_in_bytes_=*/ 1_MiB, /*max_entry_size_in_rows_=*/ 1000);
}

}

TEST(QueryResultCacheHerdTakeover, WaiterLearnsThatTheExecutorSucceeded)
{
    QueryResultCache cache = makeCache();
    const auto key = makeKey();

    auto executor = cache.acquireOrWaitHerdToken(key, long_timeout, "executor", neverCancelled());
    ASSERT_EQ(executor.outcome, Outcome::Acquired);
    ASSERT_TRUE(executor.token);

    /// The token is completed on a thread different from the one which acquired it, like a pipeline thread does.
    std::thread releaser([&] { cache.releaseHerdToken(key, executor.token, /*succeeded=*/ true); });

    auto waiter = cache.acquireOrWaitHerdToken(key, long_timeout, "waiter", neverCancelled());
    releaser.join();

    /// Either the waiter saw the finished execution, or it came after the release and became the executor itself.
    if (waiter.outcome == Outcome::Acquired)
        cache.releaseHerdToken(key, waiter.token, /*succeeded=*/ true);
    else
        EXPECT_EQ(waiter.outcome, Outcome::OwnerSucceeded);
}

TEST(QueryResultCacheHerdTakeover, WaiterTakesOverAfterTheExecutorFailed)
{
    QueryResultCache cache = makeCache();
    const auto key = makeKey();

    auto executor = cache.acquireOrWaitHerdToken(key, long_timeout, "executor", neverCancelled());
    ASSERT_EQ(executor.outcome, Outcome::Acquired);

    std::thread waiter_thread([&]
    {
        auto waiter = cache.acquireOrWaitHerdToken(key, long_timeout, "waiter", neverCancelled());
        /// Either the waiter saw the failed execution, or it came after the release and became the executor itself.
        if (waiter.outcome == Outcome::Acquired)
            cache.releaseHerdToken(key, waiter.token, /*succeeded=*/ false);
        else
            EXPECT_EQ(waiter.outcome, Outcome::OwnerFailed);
    });

    cache.releaseHerdToken(key, executor.token, /*succeeded=*/ false);
    waiter_thread.join();

    /// The key is free again, the waiter can take over.
    auto takeover = cache.acquireOrWaitHerdToken(key, long_timeout, "waiter", neverCancelled());
    ASSERT_EQ(takeover.outcome, Outcome::Acquired);
    EXPECT_NE(takeover.token, executor.token);
    cache.releaseHerdToken(key, takeover.token, /*succeeded=*/ true);
}

TEST(QueryResultCacheHerdTakeover, TimedOutWaiterKeepsTheExecutorToken)
{
    QueryResultCache cache = makeCache();
    const auto key = makeKey();

    /// The executor is still running: it does not release its token before the waiter's timeout.
    auto executor = cache.acquireOrWaitHerdToken(key, long_timeout, "executor", neverCancelled());
    ASSERT_EQ(executor.outcome, Outcome::Acquired);

    /// The waiter with a short timeout gives up and runs independently.
    EXPECT_EQ(cache.acquireOrWaitHerdToken(key, std::chrono::milliseconds(1), "impatient_waiter", neverCancelled()).outcome, Outcome::Independent);

    /// That does not dismantle the group: the token of the executor stays registered, so a later query does not become
    /// a second executor but waits for the first one (and runs independently only once its own, short, timeout elapses).
    EXPECT_EQ(cache.acquireOrWaitHerdToken(key, std::chrono::milliseconds(1), "later", neverCancelled()).outcome, Outcome::Independent);

    /// A waiter with a long timeout learns that the executor succeeded once it releases its token.
    std::atomic<Outcome> patient_outcome{Outcome::Acquired};
    std::atomic<bool> patient_started{false};
    std::thread patient([&]
    {
        patient_started = true;
        patient_outcome = cache.acquireOrWaitHerdToken(key, long_timeout, "patient_waiter", neverCancelled()).outcome;
    });
    while (!patient_started)
        std::this_thread::yield();

    /// Still the executor's token: an impatient waiter can't acquire it even while the patient waiter is waiting.
    EXPECT_EQ(cache.acquireOrWaitHerdToken(key, std::chrono::milliseconds(1), "impatient_waiter_2", neverCancelled()).outcome, Outcome::Independent);

    cache.releaseHerdToken(key, executor.token, /*succeeded=*/ true);
    patient.join();

    /// The patient waiter either waited for the executor or, if it was scheduled only after the release, became the next executor.
    const Outcome outcome = patient_outcome;
    EXPECT_TRUE(outcome == Outcome::OwnerSucceeded || outcome == Outcome::Acquired);
}

TEST(QueryResultCacheHerdTakeover, CancelledWaiterReturnsImmediately)
{
    QueryResultCache cache = makeCache();
    const auto key = makeKey();

    auto executor = cache.acquireOrWaitHerdToken(key, long_timeout, "executor", neverCancelled());
    ASSERT_EQ(executor.outcome, Outcome::Acquired);

    const auto before = std::chrono::steady_clock::now();
    EXPECT_EQ(cache.acquireOrWaitHerdToken(key, long_timeout, "waiter", []() { return true; }).outcome, Outcome::Independent);
    EXPECT_LT(std::chrono::steady_clock::now() - before, std::chrono::seconds(30));

    cache.releaseHerdToken(key, executor.token, /*succeeded=*/ true);
}

TEST(QueryResultCacheHerdTakeover, AQueryNeverWaitsForItsOwnToken)
{
    /// The Planner-level subquery path takes a token while planning and releases it only after the subquery ran, so a
    /// query containing the same subquery twice reaches `acquireOrWaitHerdToken` for the second occurrence while still
    /// owning the token of the first one. Waiting would block the query on its own unfinished computation until the timeout.
    QueryResultCache cache = makeCache();
    const auto key = makeKey(ast_hash, /*is_subquery=*/ true);

    auto own = cache.acquireOrWaitHerdToken(key, long_timeout, "query_1", neverCancelled());
    ASSERT_EQ(own.outcome, Outcome::Acquired);

    const auto before = std::chrono::steady_clock::now();
    EXPECT_EQ(cache.acquireOrWaitHerdToken(key, long_timeout, "query_1", neverCancelled()).outcome, Outcome::Independent);
    EXPECT_LT(std::chrono::steady_clock::now() - before, std::chrono::seconds(30));

    cache.releaseHerdToken(key, own.token, /*succeeded=*/ true);
}

TEST(QueryResultCacheHerdTakeover, AQueryOwningASubqueryTokenDoesNotWaitForAnotherOne)
{
    /// Query 1 plans subqueries A then B, query 2 plans B then A. If both waited, neither could ever run.
    QueryResultCache cache = makeCache();
    const auto key_a = makeKey(ast_hash, /*is_subquery=*/ true);
    const auto key_b = makeKey(other_ast_hash, /*is_subquery=*/ true);

    auto token_a = cache.acquireOrWaitHerdToken(key_a, long_timeout, "query_1", neverCancelled());
    ASSERT_EQ(token_a.outcome, Outcome::Acquired);
    auto token_b = cache.acquireOrWaitHerdToken(key_b, long_timeout, "query_2", neverCancelled());
    ASSERT_EQ(token_b.outcome, Outcome::Acquired);

    const auto before = std::chrono::steady_clock::now();
    EXPECT_EQ(cache.acquireOrWaitHerdToken(key_b, long_timeout, "query_1", neverCancelled()).outcome, Outcome::Independent);
    EXPECT_EQ(cache.acquireOrWaitHerdToken(key_a, long_timeout, "query_2", neverCancelled()).outcome, Outcome::Independent);
    EXPECT_LT(std::chrono::steady_clock::now() - before, std::chrono::seconds(30));

    cache.releaseHerdToken(key_a, token_a.token, /*succeeded=*/ true);
    cache.releaseHerdToken(key_b, token_b.token, /*succeeded=*/ true);
}

TEST(QueryResultCacheHerdTakeover, AQueryOwningOnlyItsTopLevelTokenWaitsForASubquery)
{
    /// A top-level token is only waited for by queries owning no token, so owning one does not prevent waiting.
    QueryResultCache cache = makeCache();
    const auto top_level_key = makeKey(ast_hash, /*is_subquery=*/ false);
    const auto subquery_key = makeKey(other_ast_hash, /*is_subquery=*/ true);

    auto top_level = cache.acquireOrWaitHerdToken(top_level_key, long_timeout, "query_1", neverCancelled());
    ASSERT_EQ(top_level.outcome, Outcome::Acquired);
    auto subquery = cache.acquireOrWaitHerdToken(subquery_key, long_timeout, "query_2", neverCancelled());
    ASSERT_EQ(subquery.outcome, Outcome::Acquired);

    std::thread releaser([&] { cache.releaseHerdToken(subquery_key, subquery.token, /*succeeded=*/ true); });
    auto waiter = cache.acquireOrWaitHerdToken(subquery_key, long_timeout, "query_1", neverCancelled());
    releaser.join();

    if (waiter.outcome == Outcome::Acquired)
        cache.releaseHerdToken(subquery_key, waiter.token, /*succeeded=*/ true);
    else
        EXPECT_EQ(waiter.outcome, Outcome::OwnerSucceeded);

    cache.releaseHerdToken(top_level_key, top_level.token, /*succeeded=*/ true);
}

TEST(QueryResultCacheHerdReprobe, ResultStoredBeforeTheTokenWasAcquiredIsReadFromTheCache)
{
    const auto cache = makeSharedCache();
    const auto key = makeKey();

    /// The executor has completed: its result is in the cache (the probe below finds it) and its token is gone.
    auto executor = cache->acquireOrWaitHerdToken(key, long_timeout, "executor", neverCancelled());
    ASSERT_EQ(executor.outcome, Outcome::Acquired);
    cache->releaseHerdToken(key, executor.token, /*succeeded=*/ true);

    size_t probes = 0;
    QueryResultCacheHerdTokenHolder holder;
    const bool served = coalesceQueryResultCacheHerd(
        cache, key, getContext().context, long_timeout,
        [&]
        {
            ++probes;
            return true;
        },
        holder);

    EXPECT_TRUE(served);
    EXPECT_EQ(probes, 1u);

    /// The token acquired for the probe was completed: the next identical query does not wait for it and does not time out.
    auto next = cache->acquireOrWaitHerdToken(key, std::chrono::milliseconds(1), "next", neverCancelled());
    ASSERT_EQ(next.outcome, Outcome::Acquired);
    cache->releaseHerdToken(key, next.token, /*succeeded=*/ true);
}

TEST(QueryResultCacheHerdReprobe, QueryKeepsTheTokenIfTheResultIsNotInTheCache)
{
    const auto cache = makeSharedCache();
    const auto key = makeKey();

    size_t probes = 0;
    QueryResultCacheHerdTokenHolder holder;
    const bool served = coalesceQueryResultCacheHerd(
        cache, key, getContext().context, long_timeout,
        [&]
        {
            ++probes;
            return false;
        },
        holder);

    /// The caller has to execute the query, as it did before the token was acquired...
    EXPECT_FALSE(served);
    EXPECT_EQ(probes, 1u);

    /// ... as the herd executor: identical queries wait for its token instead of becoming executors (a cancelled waiter gives up).
    EXPECT_EQ(cache->acquireOrWaitHerdToken(key, long_timeout, "waiter", []() { return true; }).outcome, Outcome::Independent);

    /// Once the query has executed, the next one becomes the executor.
    holder.release(/*succeeded=*/ true);
    auto next = cache->acquireOrWaitHerdToken(key, long_timeout, "next", neverCancelled());
    ASSERT_EQ(next.outcome, Outcome::Acquired);
    cache->releaseHerdToken(key, next.token, /*succeeded=*/ true);
}

TEST(QueryResultCacheHerdReprobe, WaiterLearnsThatTheResultWasReadFromTheCache)
{
    const auto cache = makeSharedCache();
    const auto key = makeKey();

    std::atomic<size_t> waiter_polls = 0;
    /// `Independent` is not what the waiter is expected to see, so the test fails if the waiter did not store its outcome.
    Outcome waiter_outcome = Outcome::Independent;
    std::thread waiter;

    QueryResultCacheHerdTokenHolder holder;
    const bool served = coalesceQueryResultCacheHerd(
        cache, key, getContext().context, long_timeout,
        [&]
        {
            /// The query owns the token while it probes the cache, so an identical query that arrives meanwhile waits for it.
            waiter = std::thread(
                [&]
                {
                    waiter_outcome = cache->acquireOrWaitHerdToken(
                        key, long_timeout, "waiter",
                        [&]
                        {
                            ++waiter_polls;
                            return false;
                        }).outcome;
                });

            /// A waiter polls for the cancellation every 100 ms, so once it has polled, it is known to wait for the token.
            const auto give_up_at = std::chrono::steady_clock::now() + std::chrono::seconds(60);
            while (waiter_polls == 0 && std::chrono::steady_clock::now() < give_up_at)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));

            return true;
        },
        holder);

    if (waiter.joinable())
        waiter.join();

    EXPECT_TRUE(served);

    /// The token was completed as succeeded, so the waiter reads the result from the cache instead of taking over.
    EXPECT_EQ(waiter_outcome, Outcome::OwnerSucceeded);
}
