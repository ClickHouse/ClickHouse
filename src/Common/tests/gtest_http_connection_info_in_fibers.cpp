#include <gtest/gtest.h>

#include "config.h"

#if USE_SILK

#include <Common/HTTPConnectionInfo.h>
#include <Common/Silk/FiberScheduler.h>
#include <Common/Silk/tests/gtest_silk_scheduler.h>

#include <silk/fibers/event.h>
#include <silk/fibers/future.h>

#include <atomic>
#include <thread>
#include <vector>

namespace
{

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

}

/// Each fiber publishes its own connection inside its own scope and takes it back after all fibers have published.
/// There are more fibers than scheduler threads, of which there is one per CPU, so at least two of them publish
/// on the same thread: with state per thread they would overwrite each other's connection and scope counters.
TEST(HTTPConnectionInfo, ScopesOfConcurrentFibersDoNotMix)
{
    initializeFiberSchedulerForTests();

    const size_t fiber_count = 2 * std::max(1U, std::thread::hardware_concurrency()) + 1;
    FiberBarrier published(fiber_count);
    FiberBarrier taken(fiber_count);
    std::vector<DB::HTTPConnectionInfo> results(fiber_count);
    std::vector<silk::FiberFuture> futures(fiber_count);

    /// The started fibers use the locals above, so they finish before the test returns, also when a spawn throws.
    size_t spawned = 0;
    try
    {
        while (spawned < fiber_count && Silk::spawn([&, i = spawned]
            {
                DB::HTTPConnectionInfoScope scope(/* enabled = */ true);
                DB::beginHTTPConnectionInfoRequest();
                DB::setCurrentHTTPConnectionInfo({.id = i + 1, .has_value = true});
                published.arriveAndWait();
                results[i] = DB::takeCurrentHTTPConnectionInfo();
                /// A scope that ends restores the previous state, so no scope ends before every fiber has taken its connection.
                taken.arriveAndWait();
                return 0;
            }, futures[spawned]) == 0)
            ++spawned;
    }
    catch (...)
    {
        published.dropParticipants(fiber_count - spawned);
        taken.dropParticipants(fiber_count - spawned);
        for (size_t i = 0; i < spawned; ++i)
            futures[i].wait();
        throw;
    }

    published.dropParticipants(fiber_count - spawned);
    taken.dropParticipants(fiber_count - spawned);
    for (size_t i = 0; i < spawned; ++i)
        EXPECT_EQ(futures[i].wait(), 0);
    ASSERT_EQ(spawned, fiber_count);

    for (size_t i = 0; i < fiber_count; ++i)
    {
        EXPECT_TRUE(results[i].has_value) << "fiber " << i;
        EXPECT_EQ(results[i].id, i + 1) << "fiber " << i;
    }
}

#endif
