#include <Storages/MergeTree/Streaming/MergeTreeBoundsSubscription.h>

#include <gtest/gtest.h>

#include <poll.h>

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

TEST(MergeTreeBoundsSubscription, FdIsExposed)
{
    MergeTreeBoundsSubscription sub(1, 0);
    ASSERT_GE(sub.fd(), 0);
}

TEST(MergeTreeBoundsSubscription, FdReadableAfterAdvance)
{
    MergeTreeBoundsSubscription sub(1, 0);

    /// Before any advance, fd is not readable.
    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);

    sub.advance("p1", 1);

    p = {.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/1000), 1);
    ASSERT_TRUE(p.revents & POLLIN);

    /// After drain, fd is not readable again.
    sub.drain();
    p = {.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);
}

TEST(MergeTreeBoundsSubscription, FdReadableAfterDisable)
{
    MergeTreeBoundsSubscription sub(1, 0);
    sub.disable();

    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/1000), 1);
    ASSERT_TRUE(p.revents & POLLIN);
}

TEST(MergeTreeBoundsSubscription, DefaultIsUnbounded)
{
    MergeTreeBoundsSubscription sub(1, 0);
    ASSERT_FALSE(sub.isBounded());
    ASSERT_FALSE(sub.enrichmentDone());
}

TEST(MergeTreeBoundsSubscription, EnrichmentRoundIsRecorded)
{
    MergeTreeBoundsSubscription sub(1, 0, /*bounded=*/false);
    ASSERT_FALSE(sub.enrichmentDone());

    sub.onEnrichmentRound();
    ASSERT_TRUE(sub.enrichmentDone());
}

TEST(MergeTreeBoundsSubscription, BoundedEnrichmentRoundWakesFd)
{
    MergeTreeBoundsSubscription sub(1, 0, /*bounded=*/true);
    ASSERT_TRUE(sub.isBounded());

    /// Before any enrichment, fd is not readable.
    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);

    /// An enrichment round that advanced nothing must still wake a bounded subscription,
    /// so the source can finish (e.g. over an empty table).
    sub.onEnrichmentRound();
    ASSERT_TRUE(sub.enrichmentDone());

    p = {.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/1000), 1);
    ASSERT_TRUE(p.revents & POLLIN);
}

TEST(MergeTreeBoundsSubscription, UnboundedEnrichmentRoundDoesNotWakeFd)
{
    MergeTreeBoundsSubscription sub(1, 0, /*bounded=*/false);

    /// A round that advanced nothing must not wake an unbounded subscription.
    sub.onEnrichmentRound();

    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);
}

TEST(MergeTreeBoundsSubscription, EnrichmentRoundIsNoOpAfterDisable)
{
    MergeTreeBoundsSubscription sub(1, 0, /*bounded=*/true);
    sub.disable();
    sub.drain();

    sub.onEnrichmentRound();
    ASSERT_FALSE(sub.enrichmentDone());
}
