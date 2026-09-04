#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>
#include <Storages/MergeTree/Streaming/Subscription/SubscriptionEnrichment.h>

#include <gtest/gtest.h>

#include <map>
#include <set>

using namespace DB;

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

    ASSERT_TRUE(enrichSubscription(sub, local_parts, promoters));
    ASSERT_FALSE(enrichSubscription(sub, local_parts, promoters));
    ASSERT_FALSE(enrichSubscription(sub, local_parts, promoters));
    ASSERT_TRUE(sub.snapshot().safe_block_numbers.empty());
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
    ASSERT_EQ(sub.snapshot().safe_block_numbers.at("p"), 3);
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
    ASSERT_EQ(sub.snapshot().safe_block_numbers.at("b"), 3);
    ASSERT_FALSE(sub.snapshot().safe_block_numbers.contains("a"));
}

TEST(SubscriptionEnrichment, PromoterOnlyPartitionIsNotEnriched)
{
    MergeTreeBoundsSubscription sub(1, 0);

    /// Partition "p" has an in-flight (committing) block known to the promoter but no visible local part yet.
    LocalPartsByPartition local_parts;

    std::map<String, std::set<Int64>> committing{{"p", {0}}};
    std::map<String, PartBlockNumberRanges> ranges;
    auto promoters = constructPromoters(committing, ranges);

    ASSERT_TRUE(enrichSubscription(sub, local_parts, promoters));
    ASSERT_FALSE(enrichSubscription(sub, local_parts, promoters));
    ASSERT_FALSE(enrichSubscription(sub, local_parts, promoters));
    ASSERT_TRUE(sub.snapshot().safe_block_numbers.empty());
}
