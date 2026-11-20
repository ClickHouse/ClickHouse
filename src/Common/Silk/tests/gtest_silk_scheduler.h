#pragma once

#include "config.h"

#if USE_SILK

#include <gtest/gtest.h>

#include <Common/Silk/FiberScheduler.h>

#include <silk/fibers/event.h>

#include <atomic>

/// Silk runtime only supports a single initialize/destroy per process lifetime,
/// so we initialize lazily on first usage (to avoid initializing when not running Silk tests)
/// and destroy after all tests finish.
/// Call initializeFiberSchedulerForTests from SetUpTestSuite,
/// so the scheduler starts only when a silk test actually runs.
/// The self-registered environment tears it down once after all tests finish.

inline bool silk_scheduler_initialized_for_tests = false;

inline void initializeFiberSchedulerForTests()
{
    if (silk_scheduler_initialized_for_tests)
        return;

    Silk::initializeFiberScheduler(Silk::DEFAULT_FIBER_STACK_SIZE);
    silk_scheduler_initialized_for_tests = true;
}

class SilkSchedulerTestEnvironment : public ::testing::Environment
{
public:
    void TearDown() override
    {
        if (!silk_scheduler_initialized_for_tests)
            return;

        Silk::destroyFiberScheduler();
        silk_scheduler_initialized_for_tests = false;
    }
};

/// Suspends each arriving fiber until the last one arrives.
class FiberBarrier
{
public:
    explicit FiberBarrier(size_t count) : remaining(count) {}

    void arriveAndWait()
    {
        if (--remaining == 0)
            event.set();
        else
            event.wait();
    }

    /// For participants that never started, so that the started ones are not left waiting.
    void dropParticipants(size_t count)
    {
        if (remaining.fetch_sub(count) == count)
            event.set();
    }

private:
    std::atomic<size_t> remaining;
    silk::FiberEvent event;
};

inline ::testing::Environment * const silk_scheduler_test_environment = ::testing::AddGlobalTestEnvironment(new SilkSchedulerTestEnvironment);

#endif
