#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/DistributedPlanExecutor.h>

using namespace DB;

/// The `_internal_cascades_task_limit` override may only LOWER the optimizer task budget
/// (so tests can force the fail-closed path); it must never raise it above the built-in cap,
/// otherwise a single query could disable the optimizer's work guard.
TEST(CascadesTaskLimit, OverrideOnlyLowersTheBudget)
{
    constexpr size_t default_limit = 100000;
    auto context = Context::createCopy(getContext().context);

    /// No override -> the built-in default.
    EXPECT_EQ(getCascadesTaskLimitParam(context, default_limit), default_limit);

    /// Below the cap -> used as-is.
    context->setQueryParameters({{"_internal_cascades_task_limit", "5"}});
    EXPECT_EQ(getCascadesTaskLimitParam(context, default_limit), 5u);

    /// Equal to the cap -> the cap.
    context->setQueryParameters({{"_internal_cascades_task_limit", "100000"}});
    EXPECT_EQ(getCascadesTaskLimitParam(context, default_limit), default_limit);

    /// Above the cap -> clamped back to the cap.
    context->setQueryParameters({{"_internal_cascades_task_limit", "100000000"}});
    EXPECT_EQ(getCascadesTaskLimitParam(context, default_limit), default_limit);
}
