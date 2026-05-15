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
    auto fd = sub.fd();
    ASSERT_TRUE(fd.has_value());
    ASSERT_GE(fd.value(), 0);
}

TEST(MergeTreeBoundsSubscription, FsIsNonBlocking)
{
    MergeTreeBoundsSubscription sub(1, 0);

    sub.advance("p1", 1);

    /// fd is ready after each advance.
    int64_t buf;
    auto read_bytes = ::read(sub.fd().value(), &buf, 8);
    ASSERT_EQ(read_bytes, 8);
    ASSERT_EQ(buf, 1);
}
#else
TEST(MergeTreeBoundsSubscription, FdAbsentOnNonLinux)
{
    MergeTreeBoundsSubscription sub(1, 0);
    ASSERT_FALSE(sub.fd().has_value());
}
#endif
