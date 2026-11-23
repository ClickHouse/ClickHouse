#include <gtest/gtest.h>

#include "config.h"

#if USE_SILK

#include <Common/Silk/FiberScheduler.h>
#include <Common/Silk/tests/gtest_fiber_barrier.h>
#include <Common/Silk/tests/gtest_silk_scheduler.h>
#include <Common/ThreadStatus.h>

#include <base/scope_guard.h>

#include <silk/fibers/future.h>

#include <algorithm>
#include <optional>
#include <thread>
#include <vector>

namespace
{

struct BlockerStates
{
    std::optional<bool> before_blocker;
    std::optional<bool> after_suspension;
    std::optional<bool> after_blocker;
};

}

/// Each fiber enters a blocker and suspends in it until all fibers have entered theirs. There are more fibers than
/// the threads that may run them, so at least one fiber starts on a thread where another fiber is suspended in a
/// blocker: with state per thread that fiber would start blocked.
TEST(QueryCancellationBlocker, BlockersOfConcurrentFibersDoNotMix)
{
    initializeFiberSchedulerForTests();

    const size_t fiber_count = 4 * std::max(1U, std::thread::hardware_concurrency()) + 1;
    SilkTests::FiberBarrier all_blocked(fiber_count);
    std::vector<BlockerStates> states(fiber_count);
    std::vector<silk::FiberFuture> futures(fiber_count);

    size_t spawned = 0;
    {
        /// The started fibers use the locals above, so they finish before the states are checked, also when a spawn throws.
        SCOPE_EXIT({
            all_blocked.dropParticipants(fiber_count - spawned);
            for (size_t i = 0; i < spawned; ++i)
                EXPECT_EQ(futures[i].wait(), 0);
        });

        while (spawned < fiber_count && Silk::spawn([&, i = spawned]
            {
                BlockerStates & state = states[i];
                state.before_blocker = DB::ThreadStatus::QueryCancellationBlocker::isActive();
                {
                    DB::ThreadStatus::QueryCancellationBlocker blocker;
                    all_blocked.arriveAndWait();
                    state.after_suspension = DB::ThreadStatus::QueryCancellationBlocker::isActive();
                }
                state.after_blocker = DB::ThreadStatus::QueryCancellationBlocker::isActive();
                return 0;
            }, futures[spawned]) == 0)
            ++spawned;
    }
    ASSERT_EQ(spawned, fiber_count);

    for (size_t i = 0; i < fiber_count; ++i)
    {
        EXPECT_EQ(states[i].before_blocker, false) << "fiber " << i;
        EXPECT_EQ(states[i].after_suspension, true) << "fiber " << i;
        EXPECT_EQ(states[i].after_blocker, false) << "fiber " << i;
    }
}

#endif
