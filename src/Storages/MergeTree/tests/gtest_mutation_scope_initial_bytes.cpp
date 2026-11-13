#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

using namespace DB;

static MergeTreePartInfo partInfo(const String & name)
{
    return MergeTreePartInfo::fromPartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
}

TEST(MutationScopeInitialBytes, MergedPartReplacesCoveredWeights)
{
    MutationScopeInitialBytes scope;
    scope.account(partInfo("all_1_1_0"), 100);
    scope.account(partInfo("all_2_2_0"), 100);
    EXPECT_EQ(scope.bytes, 200u);

    scope.account(partInfo("all_1_2_1"), 150);
    EXPECT_EQ(scope.bytes, 150u);
    EXPECT_EQ(scope.counted_parts.size(), 1u);

    /// A piece of a counted range keeps that range's weight.
    scope.account(partInfo("all_1_1_0"), 100);
    EXPECT_EQ(scope.bytes, 150u);
    EXPECT_EQ(scope.counted_parts.size(), 1u);
}

/// Sanitizer builds do not route operator new through the memory tracker, so the allocation cannot fail there.
#if !defined(SANITIZER)
/// A failed allocation while accounting an already counted part must not drop its weight.
TEST(MutationScopeInitialBytes, FailedAllocationKeepsCountedWeights)
{
    MainThreadStatus::getInstance();
    auto & thread_tracker = CurrentThread::get().memory_tracker;

    MutationScopeInitialBytes scope;
    const auto first = partInfo("all_1_1_0");
    scope.account(first, 100);
    scope.account(partInfo("all_2_2_0"), 100);
    ASSERT_EQ(scope.bytes, 200u);

    const Int64 saved_untracked_limit = CurrentThread::get().untracked_memory_limit;
    const Int64 saved_thread_hard_limit = thread_tracker.getHardLimit();
    const Int64 saved_total_hard_limit = total_memory_tracker.getHardLimit();
    const UInt64 saved_min_alloc_to_throw = CurrentMemoryTracker::getMinAllocationSizeBytesToThrow();

    auto lift_limits = [&]
    {
        total_memory_tracker.setHardLimit(saved_total_hard_limit);
        thread_tracker.setHardLimit(saved_thread_hard_limit);
        CurrentThread::get().untracked_memory_limit = saved_untracked_limit;
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_alloc_to_throw);
    };
    SCOPE_EXIT_SAFE(lift_limits());

    /// Make every allocation reach the trackers and fail.
    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1);
    CurrentThread::get().untracked_memory_limit = 0;
    CurrentThread::flushUntrackedMemory();
    total_memory_tracker.resetCounters();
    thread_tracker.resetCounters();
    total_memory_tracker.setHardLimit(1);
    thread_tracker.setHardLimit(1);

    bool thrown = false;
    int code = 0;
    try
    {
        scope.account(first, 100);
    }
    catch (const Exception & e)
    {
        thrown = true;
        code = e.code();
    }

    /// Lift the limits before anything else, the assertions allocate.
    lift_limits();

    ASSERT_TRUE(thrown) << "accounting was expected to fail under the memory limit";
    EXPECT_EQ(code, ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    EXPECT_EQ(scope.bytes, 200u);
    EXPECT_EQ(scope.counted_parts.size(), 2u);
    EXPECT_TRUE(scope.counted_parts.contains(first));
}
#endif
