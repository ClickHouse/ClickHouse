#include <gtest/gtest.h>

#include <Common/AsyncTaskExecutor.h>
#include <Common/CoroutineStack.h>
#include <Common/Exception.h>
#include <Common/checkStackSize.h>

#include <base/defines.h>
#include <base/getPageSize.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
}

namespace
{

using namespace DB;

/// The recursions below stop on their own measured consumption rather than on a frame count, so the
/// depth reached does not depend on how large the compiler makes a frame. The budget overshoots the
/// guard's allowance yet stops short of the end of the stack, so a build where the guard does not
/// fire fails an assertion instead of faulting.
/// Consumption is measured between frame addresses, never between two locals' addresses: under ASan
/// an address-taken local can live on the separately mapped fake stack. Each level still escapes its
/// own local's address, which is what stops the compiler folding a recursion into a frameless loop;
/// the same budget bounds the depth too, which no level that consumed a byte can reach.
constexpr size_t recursion_budget = CoroutineStack::default_stack_size * 7 / 8;

struct Observations
{
    bool ran = false;
    bool budget_reached = false;
    bool depth_capped = false;
    bool past_allowance = false;
};

size_t NO_INLINE recurseUntilGuardTrips(size_t depth, uintptr_t first_frame, Observations & observations)
{
    checkStackSize();

    char here = static_cast<char>(depth);
    __asm__ __volatile__("" : : "r"(&here) : "memory");
    if (depth >= recursion_budget)
    {
        observations.depth_capped = true;
        return depth;
    }

    if (first_frame - reinterpret_cast<uintptr_t>(__builtin_frame_address(0)) >= recursion_budget)
    {
        observations.budget_reached = true;
        return depth;
    }

    return recurseUntilGuardTrips(depth + 1, first_frame, observations) + static_cast<size_t>(here);
}

struct CheckingOnDestruction
{
    ~CheckingOnDestruction()
    {
        checkStackSize();
    }
};

/// Descends without checking, so every destructor unwound from the bottom runs deeper than the
/// guard's allowance.
size_t NO_INLINE descendThenSuspend(size_t depth, uintptr_t first_frame, Observations & observations, const SuspendCallback & suspend_callback)
{
    CheckingOnDestruction unwind_probe;

    char here = static_cast<char>(depth);
    __asm__ __volatile__("" : : "r"(&here) : "memory");
    if (depth >= recursion_budget)
    {
        observations.depth_capped = true;
        return depth;
    }

    if (first_frame - reinterpret_cast<uintptr_t>(__builtin_frame_address(0)) >= recursion_budget)
    {
        /// Establishes that the destructors above are ones a check would have thrown from, which is
        /// what makes the teardown assertions non-vacuous.
        try
        {
            checkStackSize();
        }
        catch (const Exception &)
        {
            observations.past_allowance = true;
        }

        suspend_callback();
        return depth;
    }

    return descendThenSuspend(depth + 1, first_frame, observations, suspend_callback) + static_cast<size_t>(here);
}

enum class Shape : uint8_t
{
    OrdinaryDepth,
    RecurseUntilGuardTrips,
    DescendThenSuspend,
};

struct CoroutineTask : public AsyncTask
{
    CoroutineTask(Shape shape_, Observations & observations_) : shape(shape_), observations(observations_) { }

    void run(AsyncCallback, SuspendCallback suspend_callback) override
    {
        const uintptr_t first_frame_address = reinterpret_cast<uintptr_t>(__builtin_frame_address(0));
        observations.ran = true;

        switch (shape)
        {
            case Shape::OrdinaryDepth:
                checkStackSize();
                return;
            case Shape::RecurseUntilGuardTrips:
                recurseUntilGuardTrips(0, first_frame_address, observations);
                return;
            case Shape::DescendThenSuspend:
                descendThenSuspend(0, first_frame_address, observations, suspend_callback);
                return;
        }
    }

    const Shape shape;
    Observations & observations;
};

/// Creates its coroutine the way production does, through `AsyncTaskExecutor`'s move-assignment.
class CoroutineTaskExecutor : public AsyncTaskExecutor
{
public:
    CoroutineTaskExecutor(Shape shape, Observations & observations)
        : AsyncTaskExecutor(std::make_unique<CoroutineTask>(shape, observations), "gtest_coroutine_stack_guard")
    {
    }

private:
    bool checkBeforeTaskResume() override { return true; }
    void afterTaskResume() override { }
    void processAsyncEvent(int, Poco::Timespan, AsyncEventTimeoutType, const std::string &, uint32_t) override { }
    void clearAsyncEvent() override { }
};

/// The usable stack is `default_stack_size` rounded up to a page, with the guard page on top of it
/// rather than inside it. An 8 MiB thread stack cannot satisfy this, so the assertion also rules out
/// a throw that came from the thread path against the wrong bounds.
void expectCoroutineStackBounds(const std::string & message)
{
    const std::string max_marker = "maximum stack size: ";
    const size_t max_at = message.find(max_marker);
    ASSERT_NE(max_at, std::string::npos) << message;

    const size_t reported = std::stoull(message.substr(max_at + max_marker.size()));
    EXPECT_GE(reported, CoroutineStack::default_stack_size) << message;
    EXPECT_LT(reported, CoroutineStack::default_stack_size + static_cast<size_t>(getPageSize())) << message;

    /// The comma belongs to the marker: `"stack size: "` alone also matches inside `"maximum stack size: "`.
    const std::string used_marker = ", stack size: ";
    const size_t used_at = message.find(used_marker);
    ASSERT_NE(used_at, std::string::npos) << message;
    const size_t used = std::stoull(message.substr(used_at + used_marker.size()));

    /// Mirrors `COROUTINE_STACK_RESERVE` in `checkStackSize.cpp`; pinned from the reported maximum
    /// rather than shared with it, so widening either side alone fails here.
    constexpr size_t contracted_reserve = 64 * 1024;
    ASSERT_GT(reported, contracted_reserve) << message;
    const size_t contracted_trip = reported - contracted_reserve;
    EXPECT_GE(used, contracted_trip) << message;

    /// Slack for the single recursion frame between the last check that passed and the throw.
    /// Deliberately not one OS page: where pages are 64 KiB that interval is wide enough to accept
    /// a materially smaller reserve than the one being pinned.
    constexpr size_t frame_slack = 4096;
    EXPECT_LT(used, contracted_trip + frame_slack) << message;
}

}

TEST(CoroutineStackGuard, NoThrowAtOrdinaryDepth)
{
    Observations observations;
    CoroutineTaskExecutor executor(Shape::OrdinaryDepth, observations);

    EXPECT_NO_THROW(executor.resume());
    EXPECT_TRUE(observations.ran);
}

TEST(CoroutineStackGuard, ThrowsOnDeepRecursionInsideCoroutine)
{
    Observations observations;
    CoroutineTaskExecutor executor(Shape::RecurseUntilGuardTrips, observations);

    try
    {
        executor.resume();
        /// A fatal failure returns, so the fold-specific message has to come first.
        if (observations.depth_capped)
            FAIL() << "a build folded the recursion into a frameless loop";
        FAIL() << "checkStackSize() did not stop an unbounded recursion on a coroutine stack";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::TOO_DEEP_RECURSION) << e.message();
        /// Not the bare error code: the parse-depth cap throws `TOO_DEEP_RECURSION` as well.
        ASSERT_NE(e.message().find("Stack size too large."), std::string::npos) << e.message();
        expectCoroutineStackBounds(e.message());
    }

    EXPECT_FALSE(observations.budget_reached) << "recursion ran out of budget before the guard fired";
}

TEST(CoroutineStackGuard, TeardownFromDeepStackDoesNotThrow)
{
    {
        SCOPED_TRACE("cancel()");
        Observations observations;
        CoroutineTaskExecutor executor(Shape::DescendThenSuspend, observations);
        EXPECT_NO_THROW(executor.resume());
        ASSERT_TRUE(observations.past_allowance);
        EXPECT_NO_THROW(executor.cancel());
    }
    {
        SCOPED_TRACE("restart()");
        Observations observations;
        CoroutineTaskExecutor executor(Shape::DescendThenSuspend, observations);
        EXPECT_NO_THROW(executor.resume());
        ASSERT_TRUE(observations.past_allowance);
        EXPECT_NO_THROW(executor.restart());
    }
    {
        SCOPED_TRACE("destructor");
        Observations observations;
        auto executor = std::make_unique<CoroutineTaskExecutor>(Shape::DescendThenSuspend, observations);
        EXPECT_NO_THROW(executor->resume());
        ASSERT_TRUE(observations.past_allowance);
        EXPECT_NO_THROW(executor.reset());
    }
}
