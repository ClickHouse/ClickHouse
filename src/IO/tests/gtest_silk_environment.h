#pragma once

#include "config.h"

#if USE_SILK

#include <gtest/gtest.h>

#include <Common/SilkScheduler.h>
#include <Common/tests/gtest_silk_scheduler.h>

namespace DB::tests
{

/// Process-wide Silk lifecycle for the reader-executor fiber gtests. The process supports a
/// single `silk::FiberScheduler`, shared with the `Silk::spawn`-based silk gtests: this
/// environment starts it through the same lazy helper (`initializeFiberSchedulerForTests`),
/// so whichever side initializes first wins and the other reuses it, and additionally
/// registers the reader-executor per-category fiber hooks (the `current_thread` swap for
/// `SilkFiberCategory::FETCH` fibers). Teardown is owned by the helper's self-registered
/// `SilkSchedulerTestEnvironment`. Register via `registerSilkEnvironment` — never with
/// `AddGlobalTestEnvironment` directly, so multiple test files share one registration.
class SilkTestEnvironment : public ::testing::Environment
{
public:
    void SetUp() override
    {
        registerReaderExecutorFiberHooks();
        initializeFiberSchedulerForTests();
    }
};

inline ::testing::Environment * registerSilkEnvironment()
{
    static ::testing::Environment * env = ::testing::AddGlobalTestEnvironment(new SilkTestEnvironment);
    return env;
}

}

#endif
