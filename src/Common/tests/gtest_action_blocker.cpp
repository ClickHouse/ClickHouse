#include <gtest/gtest.h>

#include <chrono>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <thread>

#include <Common/ActionBlocker.h>
#include <Common/ActionLock.h>

using namespace DB;

TEST(ActionBlocker, TestDefaultConstructor)
{
    ActionBlocker blocker;

    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_EQ(0, blocker.getCounter().load());
}

TEST(ActionBlocker, TestCancelForever)
{
    ActionBlocker blocker;

    blocker.cancelForever();
    EXPECT_TRUE(blocker.isCancelled());
    EXPECT_EQ(1, blocker.getCounter().load());
}

TEST(ActionBlocker, TestCancel)
{
    const std::string partition_id = "some partition id";
    ActionBlocker blocker;

    {
        auto lock = blocker.cancel();
        EXPECT_TRUE(blocker.isCancelled());
        EXPECT_EQ(1, blocker.getCounter().load());

        // global lock (`cancel` without partition_id) also locks all paritions.
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
    }
    // automatically un-cancelled on `lock` destruction
    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id));
}

TEST(ActionBlocker, TestCancelForPartition)
{
    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";
    ActionBlocker blocker;

    {
        auto lock = blocker.cancelForPartition(partition_id);

        // blocker is not locked fully
        EXPECT_FALSE(blocker.isCancelled());
        EXPECT_EQ(0, blocker.getCounter().load());

        // blocker reports that only partition is locked
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));

        // doesn't affect other partitions
        EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
    }

    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id));
}

TEST(ActionBlocker, TestCancelForTwoPartitions)
{
    const std::string partition_id1 = "some partition id";
    const std::string partition_id2 = "some other partition id";
    ActionBlocker blocker;

    {
        auto lock1 = blocker.cancelForPartition(partition_id1);
        auto lock2 = blocker.cancelForPartition(partition_id2);

        // blocker is not locked fully
        EXPECT_FALSE(blocker.isCancelled());
        EXPECT_EQ(0, blocker.getCounter().load());

        // blocker reports that both partitions are locked
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id1));
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id2));
    }

    // blocker is not locked fully
    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_EQ(0, blocker.getCounter().load());

    // blocker reports that only partition is locked
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id1));
}

TEST(ActionBlocker, TestCancelForSamePartitionTwice)
{
    // Partition is unlocked only when all locks are destroyed.

    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";

    // Lock `partition_id` twice, make sure that global lock
    // and other partitions are unaffected.
    // Check that `partition_id` is unlocked only after both locks are destroyed.

    ActionBlocker blocker;

    {
        auto lock1 = blocker.cancelForPartition(partition_id);
        {
            auto lock2 = blocker.cancelForPartition(partition_id);

            EXPECT_FALSE(blocker.isCancelled());
            EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
            EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
        }

        EXPECT_FALSE(blocker.isCancelled());
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
        EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
    }
    // All locks lifted

    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id));
}

TEST(ActionBlocker, TestCancelAndThenCancelForPartition)
{
    // Partition is unlocked only when all locks are destroyed.

    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";
    ActionBlocker blocker;

    {
        auto global_lock = blocker.cancel();

        {
            auto lock1 = blocker.cancelForPartition(partition_id);
            EXPECT_TRUE(blocker.isCancelled());
            EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
            EXPECT_TRUE(blocker.isCancelledForPartition(partition_id2));
        }

        EXPECT_TRUE(blocker.isCancelled());
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id2));
    }
    // All locks lifted

    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id));
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
}


TEST(ActionBlocker, TestCancelForPartitionAndThenCancel)
{
    // Partition is unlocked only when all locks are destroyed.

    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";
    ActionBlocker blocker;

    {
        auto lock1 = blocker.cancelForPartition(partition_id);
        {
            auto global_lock = blocker.cancel();

            EXPECT_TRUE(blocker.isCancelled());
            EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
            EXPECT_TRUE(blocker.isCancelledForPartition(partition_id2));
        }

        // global_locked is 'no more', so only 1 partition is locked now.
        EXPECT_FALSE(blocker.isCancelled());
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));
        EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
    }
    // All locks lifted

    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id));
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
}


TEST(ActionLock, TestDefaultConstructor)
{
    ActionLock locker;
    EXPECT_TRUE(locker.expired());
}

TEST(ActionLock, TestConstructorWithActionBlocker)
{
    ActionBlocker blocker;
    ActionLock lock(blocker);

    EXPECT_FALSE(lock.expired());
    EXPECT_TRUE(blocker.isCancelled());
    EXPECT_EQ(1, blocker.getCounter().load());
}

TEST(ActionLock, TestMoveAssignmentToEmpty)
{
    ActionBlocker blocker;

    {
        ActionLock lock;
        lock = blocker.cancel();
        EXPECT_TRUE(blocker.isCancelled());
    }
    // automatically un-cancelled on `lock` destruction
    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_EQ(0, blocker.getCounter().load());
}

TEST(ActionLock, TestMoveAssignmentToNonEmpty)
{
    ActionBlocker blocker;
    {
        auto lock = blocker.cancel();
        EXPECT_TRUE(blocker.isCancelled());

        // cause a move
        lock = blocker.cancel();

        // blocker should remain locked
        EXPECT_TRUE(blocker.isCancelled());
    }
    // automatically un-cancelled on `lock` destruction
    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_EQ(0, blocker.getCounter().load());
}

TEST(ActionLock, TestMoveAssignmentToNonEmpty2)
{
    ActionBlocker blocker1;
    ActionBlocker blocker2;
    {
        auto lock = blocker1.cancel();
        EXPECT_TRUE(blocker1.isCancelled());

        // cause a move
        lock = blocker2.cancel();

        // blocker2 be remain locked, blocker1 - unlocked
        EXPECT_TRUE(blocker2.isCancelled());

        EXPECT_FALSE(blocker1.isCancelled());
    }
    // automatically un-cancelled on `lock` destruction
    EXPECT_FALSE(blocker1.isCancelled());
    EXPECT_FALSE(blocker2.isCancelled());
    EXPECT_EQ(0, blocker1.getCounter().load());
    EXPECT_EQ(0, blocker2.getCounter().load());
}

TEST(ActionLock, TestMoveAssignmentToEmptyWithPartitionId)
{
    const std::string partition_id = "some partition id";
    ActionBlocker blocker;

    {
        ActionLock lock;
        lock = blocker.cancelForPartition(partition_id);
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));

        EXPECT_FALSE(blocker.isCancelled());
    }

    // automatically un-cancelled on `lock` destruction
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id));
    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_EQ(0, blocker.getCounter().load());
}

TEST(ActionLock, TestMoveAssignmentToNonEmptyWithPartitionId)
{
    const std::string partition_id = "some partition id";
    ActionBlocker blocker1;
    ActionBlocker blocker2;

    {
        ActionLock lock;
        lock = blocker1.cancelForPartition(partition_id);
        EXPECT_TRUE(blocker1.isCancelledForPartition(partition_id));

        EXPECT_FALSE(blocker1.isCancelled());
        EXPECT_EQ(0, blocker1.getCounter().load());
    }

    // automatically un-cancelled on `lock` destruction
    EXPECT_FALSE(blocker1.isCancelledForPartition(partition_id));
    EXPECT_FALSE(blocker1.isCancelled());
    EXPECT_EQ(0, blocker1.getCounter().load());
    EXPECT_EQ(0, blocker2.getCounter().load());
}

TEST(ActionLock, TestExpiration)
{
    ActionLock lock;
    {
        ActionBlocker blocker;
        lock = blocker.cancel();
        EXPECT_FALSE(lock.expired());
    }

    EXPECT_TRUE(lock.expired());
}

TEST(ActionLock, TestExpirationWithPartitionId)
{
    ActionLock lock;
    {
        ActionBlocker blocker;
        lock = blocker.cancelForPartition("some partition id");
        EXPECT_FALSE(lock.expired());
    }

    EXPECT_TRUE(lock.expired());
}


TEST(PartitionsLocker, DefaultConstructor)
{
    // PartitionsLocker is constructed with no partitions locked initially.
    PartitionsLocker locker;

    EXPECT_EQ(0, locker.countLockedPartitions());
}

TEST(PartitionsLocker, TestLockPartition)
{
    const std::string partition_id = "some partition id";

    PartitionsLocker locker;

    locker.lockPartition(partition_id);

    EXPECT_TRUE(locker.isPartitionLocked(partition_id));
    EXPECT_EQ(1, locker.countLockedPartitions());
}

TEST(PartitionsLocker, TestUnLockPartition)
{
    // Check that unlocking partition that wasn't locker before
    // doesn't cause any changes in visible state.

    const std::string partition_id = "some partition id";

    PartitionsLocker locker;

    locker.unLockPartition(partition_id);

    EXPECT_FALSE(locker.isPartitionLocked(partition_id));
    EXPECT_EQ(0, locker.countLockedPartitions());
}

TEST(PartitionsLocker, TestLockAndUnlockPartition)
{
    const std::string partition_id = "some partition id";

    PartitionsLocker locker;

    locker.lockPartition(partition_id);

    EXPECT_TRUE(locker.isPartitionLocked(partition_id));
    EXPECT_EQ(1, locker.countLockedPartitions());

    locker.unLockPartition(partition_id);

    EXPECT_FALSE(locker.isPartitionLocked(partition_id));
    EXPECT_EQ(0, locker.countLockedPartitions());
}

TEST(PartitionsLocker, TestLockAndUnlockPartitionMultipleTimes)
{
    const std::string partition_id = "some partition id";
    const size_t N = 100; // arbitrary number
    PartitionsLocker locker;

    // Check that partition is unlocked only after all locks were lifted.

    for (size_t i = 0; i < N; ++i)
    {
        locker.lockPartition(partition_id);

        EXPECT_TRUE(locker.isPartitionLocked(partition_id));
        EXPECT_EQ(1, locker.countLockedPartitions());
    }

    for (size_t i = 0; i < N; ++i)
    {
        EXPECT_TRUE(locker.isPartitionLocked(partition_id));
        EXPECT_EQ(1, locker.countLockedPartitions());

        locker.unLockPartition(partition_id);
    }

    EXPECT_FALSE(locker.isPartitionLocked(partition_id));
    EXPECT_EQ(0, locker.countLockedPartitions());
}
