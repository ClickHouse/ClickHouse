#include <Functions/AI/AIQuotaTracker.h>
#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

using namespace DB;

/// The AI quota tracker hangs off the query context, so that
/// `ai_function_max_api_calls_per_query` and the token limits bound a query rather than a
/// block: `executeImpl` runs once per block and once per pipeline stream, and a tracker
/// owned by any of those would hand a fresh allowance to each.
///
/// That isolation is load-bearing but implicit - it holds only because the hand-written
/// `ContextData` copy constructor omits `ai_quota_tracker`. Switching that constructor to
/// `= default`, or adding `ai_quota_tracker(o.ai_quota_tracker)` to it, would silently let
/// a new query inherit its parent's spent budget. This pins both halves of the design, so
/// such a change fails here rather than in production.
TEST(AIQuotaTracker, ScopedToTheQueryContext)
{
    auto query = Context::createCopy(getContext().context);
    query->makeQueryContext();

    auto tracker = query->getAIQuotaTracker();
    ASSERT_NE(tracker, nullptr);

    /// Created lazily once, then reused: two calls within one query must not hand out two
    /// budgets.
    EXPECT_EQ(query->getAIQuotaTracker().get(), tracker.get());

    /// A copy that becomes its own query context - the shape a distributed fragment takes -
    /// must start with a fresh budget rather than inherit this one.
    auto other_query = Context::createCopy(query);
    other_query->makeQueryContext();
    EXPECT_NE(other_query->getAIQuotaTracker().get(), tracker.get());

    /// A plain copy stays part of the same query, because `getAIQuotaTracker` resolves
    /// through `getQueryContext()`. It must share the budget, or a per-query limit would
    /// really be per subquery.
    auto subquery = Context::createCopy(query);
    EXPECT_EQ(subquery->getAIQuotaTracker().get(), tracker.get());
}
