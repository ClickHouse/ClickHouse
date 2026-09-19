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
    ActionBlocker blocker;

    {
        auto lock = blocker.cancel();
        EXPECT_TRUE(blocker.isCancelled());
        EXPECT_EQ(1, blocker.getCounter().load());
    }
    // automatically un-cancelled on `lock` destruction
    EXPECT_FALSE(blocker.isCancelled());
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
