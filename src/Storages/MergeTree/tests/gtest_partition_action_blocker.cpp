#include <gtest/gtest.h>

#include <vector>
#include <thread>
#include <fmt/format.h>

#include <Storages/MergeTree/PartitionActionBlocker.h>

#include <iomanip>

using namespace DB;

TEST(PartitionActionBlocker, TestDefaultConstructor)
{
    PartitionActionBlocker blocker;

    EXPECT_FALSE(blocker.isCancelled());
    // EXPECT_EQ(0, blocker.getCounter().load());
}

TEST(PartitionActionBlocker, TestCancelForever)
{
    PartitionActionBlocker blocker;

    blocker.cancelForever();
    EXPECT_TRUE(blocker.isCancelled());
    // EXPECT_EQ(1, blocker.getCounter().load());
}

TEST(PartitionActionBlocker, TestCancel)
{
    PartitionActionBlocker blocker;

    {
        auto lock = blocker.cancel();
        EXPECT_TRUE(blocker.isCancelled());
        // EXPECT_EQ(1, blocker.getCounter().load());
    }
    // automatically un-cancelled on `lock` destruction
    EXPECT_FALSE(blocker.isCancelled());
}

TEST(PartitionActionBlocker, TestCancelForPartition)
{
    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";
    PartitionActionBlocker blocker;

    {
        auto lock = blocker.cancelForPartition(partition_id);

        // blocker is not locked fully
        EXPECT_FALSE(blocker.isCancelled());
        // EXPECT_EQ(0, blocker.getCounter().load());

        // blocker reports that only partition is locked
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id));

        // doesn't affect other partitions
        EXPECT_FALSE(blocker.isCancelledForPartition(partition_id2));
    }

    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id));
}

TEST(PartitionActionBlocker, TestCancelForTwoPartitions)
{
    const std::string partition_id1 = "some partition id";
    const std::string partition_id2 = "some other partition id";
    PartitionActionBlocker blocker;

    {
        auto lock1 = blocker.cancelForPartition(partition_id1);
        auto lock2 = blocker.cancelForPartition(partition_id2);

        // blocker is not locked fully
        EXPECT_FALSE(blocker.isCancelled());
        // EXPECT_EQ(0, blocker.getCounter().load());

        // blocker reports that both partitions are locked
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id1));
        EXPECT_TRUE(blocker.isCancelledForPartition(partition_id2));
    }

    // blocker is not locked fully
    EXPECT_FALSE(blocker.isCancelled());
    // EXPECT_EQ(0, blocker.getCounter().load());

    // blocker reports that only partition is locked
    EXPECT_FALSE(blocker.isCancelledForPartition(partition_id1));
}

TEST(PartitionActionBlocker, TestCancelForSamePartitionTwice)
{
    // Partition is unlocked only when all locks are destroyed.

    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";

    // Lock `partition_id` twice, make sure that global lock
    // and other partitions are unaffected.
    // Check that `partition_id` is unlocked only after both locks are destroyed.

    PartitionActionBlocker blocker;

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

TEST(PartitionActionBlocker, TestCancelAndThenCancelForPartition)
{
    // Partition is unlocked only when all locks are destroyed.

    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";
    PartitionActionBlocker blocker;

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


TEST(PartitionActionBlocker, TestCancelForPartitionAndThenCancel)
{
    // Partition is unlocked only when all locks are destroyed.

    const std::string partition_id = "some partition id";
    const std::string partition_id2 = "some other partition id";
    PartitionActionBlocker blocker;

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

TEST(PartitionActionBlocker, TestAutomaticCompactPartitionBlockers)
{
    const size_t partitions_count = 100;
    const std::string partition_id = "some partition id";
    PartitionActionBlocker blocker;

    for (size_t i = 0; i < partitions_count; ++i)
    {
        blocker.cancelForPartition(partition_id + "_" + std::to_string(i));
    }

    // Automatic cleanup happens once in a while, 100 stale locks should trigger it.
    EXPECT_LT(blocker.countPartitionBlockers(), partitions_count);
}

TEST(PartitionActionBlocker, TestCompactPartitionBlockers)
{
    const size_t partitions_count = 100;
    const std::string partition_id = "some partition id";
    PartitionActionBlocker blocker;

    for (size_t i = 0; i < partitions_count; ++i)
    {
        blocker.cancelForPartition(partition_id + "_" + std::to_string(i));
    }
    // Manually cleanup all stale blockers (all blockers in this case).
    blocker.compactPartitionBlockers();

    EXPECT_EQ(0, blocker.countPartitionBlockers());
}

TEST(PartitionActionBlocker, TestCompactPartitionBlockersDoesntRemoveActiveBlockers)
{
    const size_t partitions_count = 100;
    const std::string partition_id = "some partition id";
    PartitionActionBlocker blocker;

    auto lock_foo = blocker.cancelForPartition("FOO");
    for (size_t i = 0; i < partitions_count; ++i)
    {
        blocker.cancelForPartition(partition_id + "_" + std::to_string(i));
    }
    auto lock_bar = blocker.cancelForPartition("BAR");

    EXPECT_LT(2, blocker.countPartitionBlockers());

    // Manually cleanup all stale blockers (all except held by lock_foo and lock_bar).
    blocker.compactPartitionBlockers();

    EXPECT_EQ(2, blocker.countPartitionBlockers());
}

TEST(PartitionActionBlocker, TestFormatDebug)
{
    // Do not validate contents, just make sure that something is printed out

    const size_t partitions_count = 100;
    const std::string partition_id = "some partition id";
    PartitionActionBlocker blocker;

    auto global_lock = blocker.cancel();
    auto lock_foo = blocker.cancelForPartition("FOO");
    auto lock_foo2 = blocker.cancelForPartition("FOO");
    for (size_t i = 0; i < partitions_count; ++i)
    {
        blocker.cancelForPartition(partition_id + "_" + std::to_string(i));
    }
    auto lock_bar = blocker.cancelForPartition("BAR");

    EXPECT_NE("", blocker.formatDebug());
}
