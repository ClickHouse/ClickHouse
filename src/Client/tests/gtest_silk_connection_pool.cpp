#include "config.h"

#include <gtest/gtest.h>

#if USE_SILK

#include <Client/SilkConnectionPool.h>
#include <Common/SilkFiberScheduler.h>
#include <Common/tests/gtest_silk_scheduler.h>
#include <Core/Protocol.h>
#include <Core/Settings.h>
#include <IO/ConnectionTimeouts.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

using namespace DB;

namespace
{

class SilkConnectionPoolTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        initializeFiberSchedulerForTests();
    }
};

}

/// Entries are never connected, so no server is needed.
TEST_F(SilkConnectionPoolTest, ExhaustedPoolWakesWaitingFiber)
{
    Silk::ConnectionPool pool(
        1, "localhost", 9000, "default", "default", "", "", "", "", "", "", "client",
        Protocol::Compression::Disable, Protocol::Secure::Disable, "");

    const ConnectionTimeouts timeouts;
    const Settings settings;

    ASSERT_EQ(Silk::runBlocking([&]
    {
        auto entry = pool.getUnchecked(timeouts, settings);

        silk::FiberFuture waiter;
        EXPECT_EQ(Silk::spawn([&] { return pool.getUnchecked(timeouts, settings).isNull() ? 1 : 0; }, waiter), 0);

        silk::FiberScheduler::yield();
        entry = {};

        return waiter.wait();
    }), 0);
}

#endif
