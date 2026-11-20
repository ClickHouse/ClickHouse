#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3 && USE_SILK

#include <IO/S3/Client.h>
#include <IO/S3/Requests.h>
#include <Common/Silk/FiberScheduler.h>
#include <Common/Silk/tests/gtest_silk_scheduler.h>

#include <silk/fibers/future.h>

#include <thread>
#include <vector>

using DB::AttemptsRecord;
using DB::S3::Client;

/// Each fiber marks the record of its own scope while the other fibers hold theirs. There are more fibers than scheduler
/// threads, of which there is one per CPU, so at least two of them hold a scope on the same thread: with a scope per
/// thread they would see each other's scope, and a scope that ends would restore the scope of another fiber.
TEST(S3RequestScope, ScopesOfConcurrentFibersDoNotMix)
{
    initializeFiberSchedulerForTests();

    const size_t fiber_count = 2 * std::max(1U, std::thread::hardware_concurrency()) + 1;
    FiberBarrier entered(fiber_count);
    FiberBarrier marked(fiber_count);
    std::vector<AttemptsRecord> records(fiber_count);
    std::vector<int> saw_own_scope(fiber_count, 0);
    std::vector<silk::FiberFuture> futures(fiber_count);

    /// The started fibers use the locals above, so they finish before the test returns, also when a spawn throws.
    size_t spawned = 0;
    try
    {
        while (spawned < fiber_count && Silk::spawn([&, i = spawned]
            {
                {
                    Client::RequestScope scope(&records[i]);
                    entered.arriveAndWait();
                    auto * current = Client::RequestScope::current();
                    saw_own_scope[i] = current == &scope;
                    /// Only the odd fibers have an attempt that may have been applied.
                    if (current && i % 2 == 1)
                        current->attempts_record->may_have_been_applied = true;
                    /// No scope ends before every fiber has marked its record.
                    marked.arriveAndWait();
                }
                return Client::RequestScope::current() == nullptr ? 0 : 1;
            }, futures[spawned]) == 0)
            ++spawned;
    }
    catch (...)
    {
        entered.dropParticipants(fiber_count - spawned);
        marked.dropParticipants(fiber_count - spawned);
        for (size_t i = 0; i < spawned; ++i)
            futures[i].wait();
        throw;
    }

    entered.dropParticipants(fiber_count - spawned);
    marked.dropParticipants(fiber_count - spawned);
    for (size_t i = 0; i < spawned; ++i)
        EXPECT_EQ(futures[i].wait(), 0) << "fiber " << i << " has a scope after its own ended";
    ASSERT_EQ(spawned, fiber_count);

    for (size_t i = 0; i < fiber_count; ++i)
    {
        EXPECT_TRUE(saw_own_scope[i]) << "fiber " << i;
        EXPECT_EQ(records[i].may_have_been_applied, i % 2 == 1) << "fiber " << i;
    }
}

#endif
