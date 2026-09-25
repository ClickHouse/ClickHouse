#include <gtest/gtest.h>

#include <Common/StackTrace.h>
#include <base/defines.h>

#include <csignal>
#include <optional>

namespace
{

/// The first capture from a call site unwinds by interpreting the unwind info, later ones reuse what
/// the first one found. Both must give the same frames.
StackTrace NO_INLINE captureHere()
{
    return StackTrace();
}

StackTrace NO_INLINE captureDeeper(size_t depth)
{
    if (depth == 0)
        return captureHere();
    StackTrace trace = captureDeeper(depth - 1);
    __asm__ __volatile__("" : : "r"(&trace) : "memory");
    return trace;
}

void expectSameFrames(const StackTrace & expected, const StackTrace & actual)
{
    ASSERT_EQ(expected.getSize(), actual.getSize());
    ASSERT_EQ(expected.getOffset(), actual.getOffset());
    for (size_t i = expected.getOffset(); i < expected.getSize(); ++i)
        EXPECT_EQ(expected.getFramePointers()[i], actual.getFramePointers()[i]) << "frame " << i;
}

std::optional<StackTrace> * signal_trace = nullptr;

void captureFromSignal(int, siginfo_t *, void * context)
{
    signal_trace->emplace(*static_cast<const ucontext_t *>(context));
}

int NO_INLINE raiseCaptureSignal()
{
    int result = raise(SIGUSR2);
    __asm__ __volatile__("" : : : "memory");
    return result;
}

}

/// The loops below are not unrolled: every capture has to come from the same call site.

TEST(StackTrace, RepeatedCaptureGivesSameFrames)
{
    std::optional<StackTrace> traces[4];
#pragma clang loop unroll(disable)
    for (auto & trace : traces)
        trace.emplace(captureDeeper(8));

    ASSERT_GT(traces[0]->getSize(), 9u);
    for (const auto & trace : traces)
        expectSameFrames(*traces[0], *trace);
}

TEST(StackTrace, RepeatedCaptureFromSignalGivesSameFrames)
{
    struct sigaction action{};
    struct sigaction previous{};
    action.sa_sigaction = captureFromSignal;
    action.sa_flags = SA_SIGINFO;
    ASSERT_EQ(0, sigaction(SIGUSR2, &action, &previous));

    std::optional<StackTrace> traces[2];
#pragma clang loop unroll(disable)
    for (auto & trace : traces)
    {
        signal_trace = &trace;
        ASSERT_EQ(0, raiseCaptureSignal());
    }
    ASSERT_EQ(0, sigaction(SIGUSR2, &previous, nullptr));

    ASSERT_TRUE(traces[0] && traces[1]);
    /// The capture crosses the signal frame back into the code that raised the signal.
    ASSERT_GT(traces[0]->getSize(), traces[0]->getOffset() + 1);
    expectSameFrames(*traces[0], *traces[1]);
}
