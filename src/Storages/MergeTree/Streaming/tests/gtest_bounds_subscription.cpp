#include <Storages/MergeTree/Streaming/MergeTreeBoundsSubscription.h>
#include <Storages/MergeTree/Streaming/SubscriptionEnrichment.h>

#include <gtest/gtest.h>

#include <poll.h>

#include <map>
#include <set>

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
    ASSERT_FALSE(sub.safeSegmentDetermined());
}

TEST(MergeTreeBoundsSubscription, ResolvedRoundResolvesSnapshot)
{
    MergeTreeBoundsSubscription sub(1, 0, /*bounded=*/false);
    ASSERT_FALSE(sub.safeSegmentDetermined());

    sub.onEnrichmentRound(/*pending=*/false);
    ASSERT_TRUE(sub.safeSegmentDetermined());
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
    sub.onEnrichmentRound(/*pending=*/false);
    ASSERT_TRUE(sub.safeSegmentDetermined());

    p = {.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/1000), 1);
    ASSERT_TRUE(p.revents & POLLIN);
}

TEST(MergeTreeBoundsSubscription, UnboundedEnrichmentRoundDoesNotWakeFd)
{
    MergeTreeBoundsSubscription sub(1, 0, /*bounded=*/false);

    /// A round that advanced nothing must not wake an unbounded subscription.
    sub.onEnrichmentRound(/*pending=*/false);

    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);
}

TEST(MergeTreeBoundsSubscription, EnrichmentRoundIsNoOpAfterDisable)
{
    MergeTreeBoundsSubscription sub(1, 0, /*bounded=*/true);
    sub.disable();
    sub.drain();

    sub.onEnrichmentRound(/*pending=*/false);
    ASSERT_FALSE(sub.safeSegmentDetermined());
}

TEST(MergeTreeBoundsSubscription, PendingRoundDoesNotResolve)
{
    MergeTreeBoundsSubscription sub(1, 0, /*bounded=*/true);

    /// A round that left a partition blocked must not mark the snapshot resolved - the bounded
    /// source must keep waiting rather than finish on an empty snapshot.
    sub.onEnrichmentRound(/*pending=*/true);
    ASSERT_FALSE(sub.safeSegmentDetermined());

    /// A later resolved round clears the pending state.
    sub.onEnrichmentRound(/*pending=*/false);
    ASSERT_TRUE(sub.safeSegmentDetermined());
}

TEST(MergeTreeBoundsSubscription, PendingRoundDoesNotWakeFd)
{
    MergeTreeBoundsSubscription sub(1, 0, /*bounded=*/true);

    /// A pending round has nothing for the source to do, so it must not wake it.
    sub.onEnrichmentRound(/*pending=*/true);

    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);
}

TEST(SubscriptionEnrichment, PendingWhenPromotionBlocked)
{
    MergeTreeBoundsSubscription sub(1, 0, /*bounded=*/true);

    /// Partition "p" has a visible part [5, 5], but a committing block at 2 sits in the gap
    /// below it, so promotion is blocked and the safe segment is not yet determined.
    LocalPartsByPartition local_parts;
    local_parts["p"].emplace_back("p", 5, 5, 0);

    std::map<String, std::set<Int64>> committing{{"p", {2}}};
    std::map<String, PartBlockNumberRanges> ranges;
    ranges["p"].addPart(5, 5);
    auto promoters = constructPromoters(committing, ranges);

    auto result = enrichSubscription(sub, local_parts, promoters);
    ASSERT_FALSE(result.enriched);
    ASSERT_TRUE(result.pending);
    ASSERT_TRUE(sub.snapshot().empty());
}

TEST(SubscriptionEnrichment, ResolvedWhenContiguousFromStart)
{
    MergeTreeBoundsSubscription sub(1, 0, /*bounded=*/true);

    /// Partition "p" is contiguous from the beginning, so it is fully consumed with no gap.
    LocalPartsByPartition local_parts;
    local_parts["p"].emplace_back("p", 0, 3, 0);

    std::map<String, std::set<Int64>> committing;
    std::map<String, PartBlockNumberRanges> ranges;
    ranges["p"].addPart(0, 3);
    auto promoters = constructPromoters(committing, ranges);

    auto result = enrichSubscription(sub, local_parts, promoters);
    ASSERT_TRUE(result.enriched);
    ASSERT_FALSE(result.pending);
    ASSERT_EQ(sub.snapshot().at("p"), 3);
}
