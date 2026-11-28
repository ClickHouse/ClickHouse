#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <base/scope_guard.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace DB::ErrorCodes
{
extern const int CANNOT_SCHEDULE_TASK;
}

using namespace DB;

/// The first loaded part makes the scheduling of the remaining jobs throw while its own job is still running,
/// so the running job outlives the frame of `loadStatisticsOfPartsInParallel` up to the join in the pool's destructor.
TEST(MergeTreeData, LoadStatisticsOfPartsInParallelWhenSchedulingThrows)
{
    constexpr size_t num_parts = 64;
    bool scheduling_threw = false;
    for (size_t attempt = 0; attempt < 100 && !scheduling_threw; ++attempt)
    {
        std::atomic<bool> fault_enabled = false;
        SCOPE_EXIT({ CannotAllocateThreadFaultInjector::setFaultProbability(0.0); });
        try
        {
            MergeTreeData::loadStatisticsOfPartsInParallel(
                num_parts,
                /*max_threads=*/ num_parts,
                [](size_t) { return false; },
                [&](size_t)
                {
                    if (!fault_enabled.exchange(true))
                    {
                        CannotAllocateThreadFaultInjector::setFaultProbability(1.0);
                        /// Longer than the failed scheduling takes to throw: its exception message has a symbolized stack trace.
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                    }
                },
                [](size_t) {});
        }
        catch (const Exception & e)
        {
            ASSERT_EQ(e.code(), ErrorCodes::CANNOT_SCHEDULE_TASK);
            scheduling_threw = true;
        }
    }
    EXPECT_TRUE(scheduling_threw);
}
