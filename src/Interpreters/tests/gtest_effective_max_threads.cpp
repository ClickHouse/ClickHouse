#include <gtest/gtest.h>

#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Interpreters/getEffectiveMaxThreads.h>
#include <Common/ConcurrencyControl.h>
#include <Common/MemoryTracker.h>
#include <Common/tests/gtest_global_context.h>
#include <base/scope_guard.h>

using namespace DB;

namespace
{

constexpr Int64 one_gib = 1024 * 1024 * 1024;

/// A query context with an auto `max_threads`: the number of cores, at least 2 on any machine running the tests.
ContextMutablePtr makeQueryContext()
{
    auto query_context = Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setSetting("max_threads", Field(UInt64(0)));
    return query_context;
}

}

/// A query is planned with the fair share of CPU slots taken once: other queries that start while it is
/// planned do not change it. A copied context keeps the snapshot of its query but applies its own settings.
TEST(EffectiveMaxThreads, CPUShareIsTakenOncePerQuery)
{
    auto & concurrency_control = ConcurrencyControl::instance();
    concurrency_control.setMaxConcurrency(2);
    SCOPE_EXIT({ concurrency_control.setMaxConcurrency(UnlimitedSlots); });

    auto query = makeQueryContext();
    query->setSetting("max_threads_min_free_memory_per_thread", Field(UInt64(0)));
    ASSERT_EQ(getEffectiveMaxThreads(*query), 2);

    /// Another query starts: the fair share of a new query is 1 now.
    auto other_allocation = concurrency_control.allocate(1, 1);

    EXPECT_EQ(getEffectiveMaxThreads(*query), 2);

    auto copy = Context::createCopy(query);
    EXPECT_EQ(getEffectiveMaxThreads(*copy), 2);
    copy->setSetting("max_threads", Field(UInt64(24)));
    EXPECT_EQ(getEffectiveMaxThreads(*copy), 24); /// An explicit `max_threads` is respected as is.

    auto independent_query = makeQueryContext();
    independent_query->setSetting("max_threads_min_free_memory_per_thread", Field(UInt64(0)));
    EXPECT_EQ(getEffectiveMaxThreads(*independent_query), 1);
}

/// The same for the free memory used by `max_threads_min_free_memory_per_thread`.
TEST(EffectiveMaxThreads, FreeMemoryIsTakenOncePerQuery)
{
    const Int64 old_hard_limit = total_memory_tracker.getHardLimit();
    SCOPE_EXIT({ total_memory_tracker.setHardLimit(old_hard_limit); });

    /// Half a GiB of margin, so that the memory tracked by the test itself does not change the result.
    total_memory_tracker.setHardLimit(total_memory_tracker.get() + 2 * one_gib + one_gib / 2);

    auto query = makeQueryContext();
    query->setSetting("use_concurrency_control_for_max_threads", Field(false));
    query->setSetting("max_threads_min_free_memory_per_thread", Field(UInt64(one_gib)));
    ASSERT_EQ(getEffectiveMaxThreads(*query), 2);

    /// The free memory drops.
    total_memory_tracker.setHardLimit(total_memory_tracker.get() + one_gib + one_gib / 2);

    EXPECT_EQ(getEffectiveMaxThreads(*query), 2);
    EXPECT_EQ(limitThreadsByFreeMemory(*query, 16, one_gib), 2);
    EXPECT_EQ(limitThreadsByFreeMemory(*query, 16, 2 * one_gib), 1); /// The caller's own threshold applies.
    EXPECT_EQ(getEffectiveMaxThreads(*Context::createCopy(query)), 2);

    auto independent_query = makeQueryContext();
    independent_query->setSetting("use_concurrency_control_for_max_threads", Field(false));
    independent_query->setSetting("max_threads_min_free_memory_per_thread", Field(UInt64(one_gib)));
    EXPECT_EQ(getEffectiveMaxThreads(*independent_query), 1);
}
