#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>

#include <gtest/gtest.h>

#include <poll.h>

#include <map>
#include <set>

using namespace DB;

TEST(MergeTreeBoundsSubscription, UpdateMonotonic)
{
    MergeTreeBoundsSubscription sub(1, 0);

    sub.update({{"p1", 5}}, {});
    sub.update({{"p1", 10}}, {});

    auto snap = sub.snapshot().safe_block_numbers;
    ASSERT_EQ(snap.size(), 1u);
    ASSERT_EQ(snap.at("p1"), 10);
}

TEST(MergeTreeBoundsSubscription, NewPartitionInsertedFromAbsent)
{
    MergeTreeBoundsSubscription sub(1, 0);

    sub.update({{"p1", 3}, {"p2", 7}}, {});

    auto snap = sub.snapshot().safe_block_numbers;
    ASSERT_EQ(snap.size(), 2u);
    ASSERT_EQ(snap.at("p1"), 3);
    ASSERT_EQ(snap.at("p2"), 7);
}

TEST(MergeTreeBoundsSubscription, UpdateRemovesPartitions)
{
    MergeTreeBoundsSubscription sub(1, 0);

    sub.update({{"p1", 3}, {"p2", 7}}, {});
    sub.update({}, {"p1"});

    auto snap = sub.snapshot().safe_block_numbers;
    ASSERT_EQ(snap.size(), 1u);
    ASSERT_EQ(snap.at("p2"), 7);
}

TEST(MergeTreeBoundsSubscription, DisablePreventsUpdate)
{
    MergeTreeBoundsSubscription sub(1, 0);
    sub.update({{"p1", 5}}, {});

    sub.disable();
    ASSERT_TRUE(sub.isDisabled());

    /// Should be a no-op.
    sub.update({{"p1", 10}}, {});

    auto snap = sub.snapshot().safe_block_numbers;
    ASSERT_EQ(snap.at("p1"), 5);
}

TEST(MergeTreeBoundsSubscription, FdIsExposed)
{
    MergeTreeBoundsSubscription sub(1, 0);
    ASSERT_GE(sub.fd(), 0);
}

TEST(MergeTreeBoundsSubscription, UpdateWithChangesWakesFd)
{
    MergeTreeBoundsSubscription sub(1, 0);

    /// Before any update, fd is not readable.
    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);

    sub.update({{"p1", 1}}, {});

    p = {.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/1000), 1);
    ASSERT_TRUE(p.revents & POLLIN);

    /// After drain, fd is not readable again.
    sub.drain();
    p = {.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);

    /// A removal wakes readers too.
    sub.update({}, {"p1"});
    p = {.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/1000), 1);
}

TEST(MergeTreeBoundsSubscription, FdReadableAfterDisable)
{
    MergeTreeBoundsSubscription sub(1, 0);
    sub.disable();

    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/1000), 1);
    ASSERT_TRUE(p.revents & POLLIN);
}

TEST(MergeTreeBoundsSubscription, EmptyUpdateWakesOnlyFirstTime)
{
    MergeTreeBoundsSubscription sub(1, 0);
    ASSERT_FALSE(sub.snapshot().was_updated);

    /// The first update wakes readers even when empty — a source over a shard with
    /// no partitions learns there is nothing to wait for.
    sub.update({}, {});
    ASSERT_TRUE(sub.snapshot().was_updated);
    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/1000), 1);
    sub.drain();

    /// Subsequent empty updates do not wake readers.
    sub.update({}, {});
    p = {.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);
}

TEST(MergeTreeBoundsSubscription, UpdateIsNoOpAfterDisable)
{
    MergeTreeBoundsSubscription sub(1, 0);
    sub.disable();
    sub.drain();

    sub.update({{"p1", 1}}, {});
    ASSERT_FALSE(sub.snapshot().was_updated);
    ASSERT_TRUE(sub.snapshot().safe_block_numbers.empty());

    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);
}
