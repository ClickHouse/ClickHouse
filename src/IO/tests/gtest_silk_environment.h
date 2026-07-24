#pragma once

#include "config.h"

#if USE_SILK

#include <gtest/gtest.h>

#include <Common/SilkSchedulerOptions.h>

#include <silk/fibers/fiber.h>
#include <silk/util/init.h>

namespace DB::tests
{

/// Process-wide Silk lifecycle for unit tests. Register via
/// `registerSilkEnvironment` — never with AddGlobalTestEnvironment directly:
/// `FiberScheduler::initialize` aborts on double-init, and gtest runs every
/// registered environment regardless of the test filter.
class SilkTestEnvironment : public ::testing::Environment
{
public:
    void SetUp() override
    {
        silk::initialize();
        /// Reuse the server's options (stack size + the current_thread-swapping
        /// fiber-switch hooks) so the hooks get exercised by the existing gtests
        /// instead of only running against a bare `Options` that never wires them.
        silk::FiberScheduler::Options options = makeServerSilkSchedulerOptions();
        silk::FiberScheduler::initialize(&options);
    }

    void TearDown() override
    {
        silk::FiberScheduler::destroy();
        silk::destroy();
    }
};

inline ::testing::Environment * registerSilkEnvironment()
{
    static ::testing::Environment * env = ::testing::AddGlobalTestEnvironment(new SilkTestEnvironment);
    return env;
}

}

#endif
