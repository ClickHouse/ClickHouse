#include <Storages/MergeTree/Streaming/MergeTreeBoundsSubscription.h>
#include <Storages/MergeTree/Streaming/SubscriptionEnrichment.h>
#include <Storages/Streaming/SubscriptionManager.h>

#include <gtest/gtest.h>

#include <poll.h>

#include <map>
#include <memory>
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

TEST(MergeTreeBoundsSubscription, AdvanceDoesNotWakeFd)
{
    MergeTreeBoundsSubscription sub(1, 0);

    /// Readers are woken per enrichment round, not per per-partition advance, to avoid a partial mid-round read.
    sub.advance("p1", 1);

    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);

    /// The advance still updated the safe block number.
    ASSERT_EQ(sub.snapshot().at("p1"), 1);
}

TEST(MergeTreeBoundsSubscription, FdReadableAfterEnrichmentRound)
{
    MergeTreeBoundsSubscription sub(1, 0);

    /// Before any enrichment round, fd is not readable.
    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);

    /// A round that advanced nothing must still wake readers (e.g. empty table).
    sub.advance("p1", 1);
    sub.onEnrichmentRound();

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

TEST(MergeTreeBoundsSubscription, UpdatesCountStartsAtZero)
{
    MergeTreeBoundsSubscription sub(1, 0);
    ASSERT_EQ(sub.updatesCount(), 0u);
}

TEST(MergeTreeBoundsSubscription, UpdatesCountIncrementsPerRound)
{
    MergeTreeBoundsSubscription sub(1, 0);

    /// An empty round counts too — it is the signal a bounded source waits for.
    sub.onEnrichmentRound();
    ASSERT_EQ(sub.updatesCount(), 1u);

    sub.advance("p1", 1);
    sub.onEnrichmentRound();
    ASSERT_EQ(sub.updatesCount(), 2u);
}

TEST(MergeTreeBoundsSubscription, EnrichmentRoundIsNoOpAfterDisable)
{
    MergeTreeBoundsSubscription sub(1, 0);
    sub.disable();
    sub.drain();

    sub.onEnrichmentRound();
    ASSERT_EQ(sub.updatesCount(), 0u);

    pollfd p{.fd = sub.fd(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(::poll(&p, 1, /*timeout_ms=*/0), 0);
}

TEST(StreamSubscriptionManager, CollectReturnsLiveSubscriptions)
{
    StreamSubscriptionManager manager;

    auto first = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
    auto second = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
    manager.registerSubscription(first);
    manager.registerSubscription(second);

    ASSERT_FALSE(manager.isEmpty());
    ASSERT_EQ(manager.collectSubscriptions().size(), 2u);
}

TEST(StreamSubscriptionManager, CollectSkipsExpiredSubscriptions)
{
    StreamSubscriptionManager manager;

    auto alive = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
    manager.registerSubscription(alive);
    manager.registerSubscription(std::make_shared<MergeTreeBoundsSubscription>(1, 0));

    /// The manager holds weak pointers, so the temporary above is already gone.
    auto collected = manager.collectSubscriptions();
    ASSERT_EQ(collected.size(), 1u);
    ASSERT_EQ(collected.front().get(), alive.get());
}

TEST(StreamSubscriptionManager, CollectKeepsSubscriptionAlive)
{
    StreamSubscriptionManager manager;

    std::weak_ptr<IStreamSubscription> weak;
    std::vector<StreamSubscriptionPtr> collected;
    {
        auto subscription = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
        weak = subscription;
        manager.registerSubscription(subscription);
        collected = manager.collectSubscriptions();
    }

    /// The collected strong pointer must outlive the caller that registered it, so a round cannot
    /// run against a half-destroyed subscription.
    ASSERT_EQ(collected.size(), 1u);
    ASSERT_FALSE(weak.expired());

    collected.clear();
    ASSERT_TRUE(weak.expired());
}

TEST(StreamSubscriptionManager, CollectExcludesLaterRegistrations)
{
    StreamSubscriptionManager manager;

    auto first = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
    manager.registerSubscription(first);

    auto collected = manager.collectSubscriptions();
    ASSERT_EQ(collected.size(), 1u);

    /// A subscription registered after the collect must not be served by this round: the caller
    /// reads the parts only after collecting, so that snapshot may predate this subscription.
    auto second = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
    manager.registerSubscription(second);

    ASSERT_EQ(collected.size(), 1u);
    ASSERT_EQ(collected.front().get(), first.get());

    /// It is picked up by the next round instead.
    ASSERT_EQ(manager.collectSubscriptions().size(), 2u);
}

TEST(SubscriptionEnrichment, NotEnrichedWhenPromotionBlocked)
{
    MergeTreeBoundsSubscription sub(1, 0);

    /// Part [5, 5] is visible but a committing block at 2 sits in the gap below it, so promotion is blocked.
    LocalPartsByPartition local_parts;
    local_parts["p"].emplace_back("p", 5, 5, 0);

    std::map<String, std::set<Int64>> committing{{"p", {2}}};
    std::map<String, PartBlockNumberRanges> ranges;
    ranges["p"].addPart(5, 5);
    auto promoters = constructPromoters(committing, ranges);

    ASSERT_FALSE(enrichSubscription(sub, local_parts, promoters));
    ASSERT_TRUE(sub.snapshot().empty());
}

TEST(SubscriptionEnrichment, EnrichedWhenContiguousFromStart)
{
    MergeTreeBoundsSubscription sub(1, 0);

    /// Partition "p" is contiguous from the beginning, so it is fully consumed with no gap.
    LocalPartsByPartition local_parts;
    local_parts["p"].emplace_back("p", 0, 3, 0);

    std::map<String, std::set<Int64>> committing;
    std::map<String, PartBlockNumberRanges> ranges;
    ranges["p"].addPart(0, 3);
    auto promoters = constructPromoters(committing, ranges);

    ASSERT_TRUE(enrichSubscription(sub, local_parts, promoters));
    ASSERT_EQ(sub.snapshot().at("p"), 3);
}

TEST(SubscriptionEnrichment, PartialRoundAdvancesReadablePartition)
{
    MergeTreeBoundsSubscription sub(1, 0);

    /// "b" is readable but "a" has an in-flight block in its gap, so the round advances "b" and leaves "a" untouched.
    LocalPartsByPartition local_parts;
    local_parts["a"].emplace_back("a", 5, 5, 0);
    local_parts["b"].emplace_back("b", 0, 3, 0);

    std::map<String, std::set<Int64>> committing{{"a", {2}}};
    std::map<String, PartBlockNumberRanges> ranges;
    ranges["a"].addPart(5, 5);
    ranges["b"].addPart(0, 3);
    auto promoters = constructPromoters(committing, ranges);

    ASSERT_TRUE(enrichSubscription(sub, local_parts, promoters));
    ASSERT_EQ(sub.snapshot().at("b"), 3);
    ASSERT_FALSE(sub.snapshot().contains("a"));
}

TEST(SubscriptionEnrichment, PromoterOnlyPartitionIsNotEnriched)
{
    MergeTreeBoundsSubscription sub(1, 0);

    /// Partition "p" has an in-flight (committing) block known to the promoter but no visible local part yet.
    LocalPartsByPartition local_parts;

    std::map<String, std::set<Int64>> committing{{"p", {0}}};
    std::map<String, PartBlockNumberRanges> ranges;
    auto promoters = constructPromoters(committing, ranges);

    ASSERT_FALSE(enrichSubscription(sub, local_parts, promoters));
    ASSERT_TRUE(sub.snapshot().empty());
}
