#pragma once

#include "config.h"

#if USE_SILK

#include <gtest/gtest.h>

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
        silk::FiberScheduler::Options options;
        /// OpenSSL handshakes run on fiber stacks and need more room than the silk default.
        options.fiberStackSize = 320 * 1024;
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
