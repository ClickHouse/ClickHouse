#include <Common/OpenTelemetryTraceContext.h>

#include <gtest/gtest.h>

#include <thread>

using namespace DB::OpenTelemetry;

TEST(OpenTelemetryTracingContext, TracingContextHolderPreservesParentTraceFlags)
{
    TracingContextOnThread parent;
    parent.trace_id = TracingContext::generateTraceId();
    parent.span_id = TracingContext::generateSpanId();
    parent.trace_flags = TRACE_FLAG_SAMPLED | TRACE_FLAG_KEEPER_SPANS;

    /// TracingContextHolder should be created at the start of a thread, so run the check on a fresh one.
    std::thread([&parent]
    {
        TracingContextHolder holder("test_operation", parent);

        const auto & current = CurrentContext();
        EXPECT_TRUE(current.isTraceEnabled());
        EXPECT_EQ(current.trace_id, parent.trace_id);
        EXPECT_EQ(current.trace_flags, parent.trace_flags);
    }).join();
}

TEST(OpenTelemetryTracingContext, TracingContextHolderForcesSampledFlag)
{
    TracingContextOnThread parent;
    parent.trace_id = TracingContext::generateTraceId();
    parent.span_id = TracingContext::generateSpanId();
    parent.trace_flags = TRACE_FLAG_KEEPER_SPANS;

    std::thread([&parent]
    {
        TracingContextHolder holder("test_operation", parent);

        const auto & current = CurrentContext();
        EXPECT_TRUE(current.isTraceEnabled());
        EXPECT_EQ(current.trace_flags, TRACE_FLAG_SAMPLED | TRACE_FLAG_KEEPER_SPANS);
    }).join();
}
