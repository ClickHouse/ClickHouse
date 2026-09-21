#include "config.h"

#include <gtest/gtest.h>

#if USE_SILK

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ProfileEvents.h>
#include <Common/SilkFiberScheduler.h>
#include <Common/SilkFiberTaskContext.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_silk_scheduler.h>
#include <Core/UUID.h>

#include <base/defines.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <memory>
#include <optional>
#include <tuple>
#include <vector>

namespace ProfileEvents
{
    extern const Event SelectedRows;
}

namespace
{

class SilkFiberTaskContextTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        initializeFiberSchedulerForTests();
    }
};

/// `current_thread` is a fiber-local: read it out of line, so that the compiler cannot cache its
/// address across a suspension point.
NO_INLINE DB::ThreadGroupPtr loadCurrentGroup()
{
    return DB::CurrentThread::getGroup();
}

NO_INLINE DB::OpenTelemetry::TracingContextOnThread loadCurrentTraceContext()
{
    return DB::OpenTelemetry::CurrentContext();
}

DB::OpenTelemetry::TracingContextOnThread makeTracedContext()
{
    DB::OpenTelemetry::TracingContextOnThread context;
    context.trace_id = DB::UUIDHelpers::generateV4();
    context.span_id = 0xC0FFEE;
    context.trace_flags = DB::OpenTelemetry::TRACE_FLAG_SAMPLED;
    return context;
}

}


TEST_F(SilkFiberTaskContextTest, FiberWithoutContextRunsDetached)
{
    std::optional<bool> has_group;
    std::optional<bool> trace_enabled;
    EXPECT_EQ(Silk::runBlocking([&]() -> int
    {
        has_group = loadCurrentGroup() != nullptr;
        trace_enabled = loadCurrentTraceContext().isTraceEnabled();
        return 0;
    }, Silk::FiberTaskContext{}), 0);

    ASSERT_TRUE(has_group.has_value());
    EXPECT_FALSE(*has_group);
    ASSERT_TRUE(trace_enabled.has_value());
    EXPECT_FALSE(*trace_enabled);
}

TEST_F(SilkFiberTaskContextTest, FiberJoinsTheThreadGroup)
{
    /// A group is created by a thread with a `ThreadStatus`; the test thread stays detached from it.
    DB::ThreadStatus thread_status;
    auto group = std::make_shared<DB::ThreadGroup>(getContext().context, 0);
    const auto rows_before = group->performance_counters[ProfileEvents::SelectedRows];

    constexpr Int64 allocation = 8 << 20;
    std::optional<bool> same_group;
    std::optional<Int64> group_memory_while_allocated;

    Silk::FiberTaskContext context;
    context.thread_group = group;

    EXPECT_EQ(Silk::runBlocking([&]() -> int
    {
        same_group = loadCurrentGroup() == group;

        /// Memory allocated on the fiber is charged to the group's memory tracker.
        std::ignore = CurrentMemoryTracker::alloc(allocation);
        DB::CurrentThread::flushUntrackedMemory();
        group_memory_while_allocated = group->memory_tracker.get();
        std::ignore = CurrentMemoryTracker::free(allocation);
        DB::CurrentThread::flushUntrackedMemory();

        /// Profile events too.
        ProfileEvents::increment(ProfileEvents::SelectedRows, 42);
        return 0;
    }, context), 0);

    ASSERT_TRUE(same_group.has_value());
    EXPECT_TRUE(*same_group);
    ASSERT_TRUE(group_memory_while_allocated.has_value());
    EXPECT_GE(*group_memory_while_allocated, allocation);
    EXPECT_EQ(group->performance_counters[ProfileEvents::SelectedRows], rows_before + 42);
}

TEST_F(SilkFiberTaskContextTest, FiberSpanIsAChildOfTheCapturedContext)
{
    const auto parent = makeTracedContext();

    Silk::FiberTaskContext context;
    context.trace_context = parent;
    context.operation_name = "test fiber";

    std::optional<DB::OpenTelemetry::TracingContextOnThread> inside;
    EXPECT_EQ(Silk::runBlocking([&]() -> int
    {
        inside = loadCurrentTraceContext();
        return 0;
    }, context), 0);

    ASSERT_TRUE(inside.has_value());
    EXPECT_TRUE(inside->isTraceEnabled());
    EXPECT_EQ(inside->trace_id, parent.trace_id);
    /// The fiber runs under its own root span, whose parent is the captured span.
    EXPECT_NE(inside->span_id, 0u);
    EXPECT_NE(inside->span_id, parent.span_id);

    /// The calling thread's context is untouched.
    EXPECT_FALSE(DB::OpenTelemetry::CurrentContext().isTraceEnabled());
}

TEST_F(SilkFiberTaskContextTest, CaptureTakesTheGroupAndTraceOfTheCaller)
{
    DB::ThreadStatus thread_status;
    auto group = std::make_shared<DB::ThreadGroup>(getContext().context, 0);
    DB::CurrentThread::attachToGroup(group);
    SCOPE_EXIT_SAFE(DB::CurrentThread::detachFromGroupIfNotDetached());

    const auto parent = makeTracedContext();
    DB::OpenTelemetry::TracingContextHolder tracing_holder("test thread", parent);
    const auto thread_span_id = DB::OpenTelemetry::CurrentContext().span_id;
    ASSERT_NE(thread_span_id, 0u);

    const auto captured = Silk::FiberTaskContext::capture("test fiber");
    EXPECT_EQ(captured.thread_group, group);
    EXPECT_EQ(captured.trace_context.trace_id, parent.trace_id);
    EXPECT_EQ(captured.trace_context.span_id, thread_span_id);
    EXPECT_EQ(captured.operation_name, "test fiber");

    std::optional<bool> same_group;
    std::optional<UInt64> fiber_parent_span_id;
    EXPECT_EQ(Silk::runBlocking([&]() -> int
    {
        same_group = loadCurrentGroup() == group;
        /// Inside the fiber the current span is the fiber's root span; its parent is the thread's span,
        /// which is what `SpanHolder`s created on the fiber will chain from.
        fiber_parent_span_id = loadCurrentTraceContext().span_id;
        return 0;
    }, captured), 0);

    ASSERT_TRUE(same_group.has_value());
    EXPECT_TRUE(*same_group);
    ASSERT_TRUE(fiber_parent_span_id.has_value());
    EXPECT_NE(*fiber_parent_span_id, thread_span_id);

    /// Nothing leaked back onto the calling thread.
    EXPECT_EQ(DB::CurrentThread::getGroup(), group);
    EXPECT_EQ(DB::OpenTelemetry::CurrentContext().span_id, thread_span_id);
}

TEST_F(SilkFiberTaskContextTest, ManyFibersShareOneGroup)
{
    DB::ThreadStatus thread_status;
    auto group = std::make_shared<DB::ThreadGroup>(getContext().context, 0);
    const auto rows_before = group->performance_counters[ProfileEvents::SelectedRows];

    Silk::FiberTaskContext context;
    context.thread_group = group;

    constexpr size_t num_fibers = 64;
    std::vector<silk::FiberFuture> futures(num_fibers);
    for (auto & future : futures)
    {
        ASSERT_EQ(Silk::spawn([]() -> int
        {
            for (int i = 0; i < 4; ++i)
            {
                ProfileEvents::increment(ProfileEvents::SelectedRows, 1);
                silk::FiberScheduler::yield();
            }
            return 0;
        }, future, context), 0);
    }
    for (auto & future : futures)
        EXPECT_EQ(future.wait(), 0);

    EXPECT_EQ(group->performance_counters[ProfileEvents::SelectedRows], rows_before + num_fibers * 4);
}

#endif
