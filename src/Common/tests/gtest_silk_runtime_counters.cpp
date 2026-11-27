#include "config.h"

#include <gtest/gtest.h>

#if USE_SILK

#include <Common/SilkFiberScheduler.h>
#include <Common/tests/gtest_silk_scheduler.h>

#include <silk/fibers/future.h>

#include <cstdint>
#include <string_view>

namespace
{

class SilkRuntimeCountersTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        initializeFiberSchedulerForTests();
    }
};

uint64_t counterValue(const Silk::RuntimeCounters & counters, std::string_view name)
{
    for (const auto & [counter_name, counter_value] : counters)
        if (counter_name == name)
            return counter_value;
    ADD_FAILURE() << "Counter " << name << " not found";
    return 0;
}

}


TEST_F(SilkRuntimeCountersTest, FiberActivityIsObservable)
{
    const uint64_t started_before = counterValue(Silk::getRuntimeCounters(), "FiberStarted");

    constexpr uint64_t fibers = 5;
    for (uint64_t i = 0; i < fibers; ++i)
    {
        silk::FiberFuture future;
        ASSERT_EQ(Silk::spawn([] { return 0; }, future), 0);
        EXPECT_EQ(future.wait(), 0);
    }

    /// The FiberStarted atomic is guaranteed to be incremented on the current thread,
    /// so there is a happens-before.
    const uint64_t started_after = counterValue(Silk::getRuntimeCounters(), "FiberStarted");
    EXPECT_GE(started_after - started_before, fibers);
}

#endif
