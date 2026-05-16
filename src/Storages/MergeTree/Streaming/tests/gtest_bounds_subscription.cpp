#include <Storages/MergeTree/Streaming/MergeTreeBoundsSubscription.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(MergeTreeBoundsSubscription, AdvanceMonotonic)
{
    MergeTreeBoundsSubscription sub(1, 0);

    sub.advance("p1", 5);
    sub.advance("p1", 10);

    auto snap = sub.snapshot();
    ASSERT_EQ(snap.size(), 1u);
    ASSERT_EQ(snap.at("p1"), 10);
}

TEST(MergeTreeBoundsSubscription, NewPartitionInsertedFromAbsent)
{
    MergeTreeBoundsSubscription sub(1, 0);

    sub.advance("p1", 3);
    sub.advance("p2", 7);

    auto snap = sub.snapshot();
    ASSERT_EQ(snap.size(), 2u);
    ASSERT_EQ(snap.at("p1"), 3);
    ASSERT_EQ(snap.at("p2"), 7);
}

TEST(MergeTreeBoundsSubscription, DisablePreventsAdvance)
{
    MergeTreeBoundsSubscription sub(1, 0);
    sub.advance("p1", 5);

    sub.disable();
    ASSERT_TRUE(sub.isDisabled());

    /// Should be a no-op.
    sub.advance("p1", 10);

    auto snap = sub.snapshot();
    ASSERT_EQ(snap.at("p1"), 5);
}

#if defined(OS_LINUX)
TEST(MergeTreeBoundsSubscription, FdIsExposedOnLinux)
{
    MergeTreeBoundsSubscription sub(1, 0);
    auto * fd = sub.fd();
    ASSERT_TRUE(fd != nullptr);
    ASSERT_GE(fd->fd, 0);
}

TEST(MergeTreeBoundsSubscription, FsIsNonBlocking)
{
    MergeTreeBoundsSubscription sub(1, 0);

    sub.advance("p1", 1);
    sub.advance("p1", 2);
    sub.advance("p1", 3);

    /// fd is ready after each advance.
    ASSERT_EQ(sub.fd()->read(), 3);

    /// fd is non-blocking
    ASSERT_EQ(sub.fd()->read(), 0);
    ASSERT_EQ(sub.fd()->read(), 0);
    ASSERT_EQ(sub.fd()->read(), 0);
    ASSERT_EQ(sub.fd()->read(), 0);
}
#else
TEST(MergeTreeBoundsSubscription, FdAbsentOnNonLinux)
{
    MergeTreeBoundsSubscription sub(1, 0);
    ASSERT_FALSE(sub.fd().has_value());
}
#endif
