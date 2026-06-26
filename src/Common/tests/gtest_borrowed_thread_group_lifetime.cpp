#include <thread>

#include <gtest/gtest.h>

#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

namespace ProfileEvents
{
    extern const Event Query;
}

/// Regression test for the borrowed-ThreadGroup use-after-free.
///
/// A "borrowed" child group (createForMaterializedView / createForFlushAsyncInsertQueue) stores RAW
/// pointers into its parent group's performance_counters / memory_tracker. The documented invariant
/// (MemoryTracker.h: "Lifetime of these trackers should include lifetime of current tracker") was not
/// enforced: nothing kept the parent alive, so a child pinned past the source query's end (e.g. by a
/// MergeTreeDeduplicationLog writer's captured thread-pool scheduler under s3_allow_parallel_part_upload)
/// outlived its parent. The next counter increment / MemoryTracker::setParent then walked freed memory.
///
/// The fix makes the borrowed child own a shared_ptr to its parent. These tests drop every EXTERNAL
/// reference to the parent and then exercise the borrowed pointers; without the fix this is a UAF that
/// ASan/TSan catch, with the fix the parent stays alive for exactly as long as the child needs it.

namespace DB
{

/// Dropping the only external shared_ptr to the parent must not free the parent group while a borrowed
/// child is still alive: the child keeps it referenced (use_count stays >= the child's hold).
TEST(BorrowedThreadGroupLifetime, ChildKeepsParentAlive)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto parent = std::make_shared<ThreadGroup>(context, 0);
        std::weak_ptr<ThreadGroup> parent_weak = parent;

        /// Borrowed child: holds raw pointers into parent->performance_counters / parent->memory_tracker.
        auto child = std::make_shared<ThreadGroup>(parent);

        /// Drop the only external owner of the parent. With the fix the child still owns it.
        parent.reset();

        EXPECT_FALSE(parent_weak.expired())
            << "Borrowed child must keep its parent ThreadGroup alive (raw counter pointers would dangle otherwise)";
    });
    t.join();
}

/// The classic crash shape: increment a counter through a borrowed child after the external parent
/// reference is gone. increment() walks current->parent up the chain into the parent group's Counters.
/// If the parent were freed, this reads freed memory. With the fix the parent is alive, so the walk is safe.
TEST(BorrowedThreadGroupLifetime, IncrementThroughBorrowedChildAfterParentDropped)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto parent = std::make_shared<ThreadGroup>(context, 0);
        auto child = std::make_shared<ThreadGroup>(parent);

        /// Only the child references the parent now.
        parent.reset();

        /// Walks child->performance_counters then up into the (still-alive) parent group's counters.
        child->performance_counters.increment(ProfileEvents::Query, 1);
        child->performance_counters.incrementNoTrace(ProfileEvents::Query, 1);

        SUCCEED();
    });
    t.join();
}

/// Same lifetime hazard exercised through the real attach path: attachToGroupImpl calls
/// performance_counters.setParent(&thread_group->performance_counters) and
/// memory_tracker.setParent(&thread_group->memory_tracker), i.e. it dereferences the borrowed child's
/// parent counters. Attaching to a borrowed child whose external parent reference is gone must be safe.
TEST(BorrowedThreadGroupLifetime, AttachToBorrowedChildAfterParentDropped)
{
    std::thread t([&]
    {
        ThreadStatus ts;
        auto context = getContext().context;

        auto parent = std::make_shared<ThreadGroup>(context, 0);
        auto child = std::make_shared<ThreadGroup>(parent);
        parent.reset();

        CurrentThread::attachToGroupIfDetached(child);
        /// Allocate a little so the memory tracker chain (child -> parent group's tracker) is walked.
        std::vector<int> v(1024, 7);
        ProfileEvents::increment(ProfileEvents::Query, 1);
        CurrentThread::detachFromGroupIfNotDetached();

        EXPECT_EQ(getCurrentThreadGroup(), nullptr);
    });
    t.join();
}

} // namespace DB
