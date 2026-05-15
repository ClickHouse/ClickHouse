#include <Storages/MergeTree/Streaming/CursorPromoter.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

MergeTreeCursorPromoter makePromoter(
    std::set<Int64> committing,
    std::vector<std::pair<Int64, Int64>> ranges)
{
    PartBlockNumberRanges r;
    for (auto [l, h] : ranges)
        r.addPart(l, h);
    return MergeTreeCursorPromoter(std::move(committing), std::move(r));
}

}

TEST(CursorPromoter, CanPromoteAcrossClearGap)
{
    /// Cursor at 1 (we have local part [1,1]), next local part [3,7].
    /// Nothing pending in (1, 3) — promote OK.
    auto promoter = makePromoter({}, {{1, 1}, {3, 7}});
    ASSERT_TRUE(promoter.canPromote(1, 3));
}

TEST(CursorPromoter, CanNotPromoteOverPendingPartInGap)
{
    /// Virtual has [1,1], [2,2], [3,3] but local has only [1,1] and [3,3].
    /// Pending part [2,2] sits in the gap (1, 3) — block.
    auto promoter = makePromoter({}, {{1, 1}, {2, 2}, {3, 3}});
    ASSERT_FALSE(promoter.canPromote(1, 3));
}

TEST(CursorPromoter, CanNotPromoteOverCommittingBlockInGap)
{
    /// Keeper-allocated block 2 in (1, 3) — block.
    auto promoter = makePromoter({2}, {{1, 1}, {3, 7}});
    ASSERT_FALSE(promoter.canPromote(1, 3));
}

TEST(CursorPromoter, CommittingBlockOutsideGapDoesNotBlock)
{
    /// Committing block 15 is past our promotion target — doesn't constrain.
    auto promoter = makePromoter({15}, {{1, 1}, {3, 7}});
    ASSERT_TRUE(promoter.canPromote(1, 3));
}

TEST(CursorPromoter, CanNotPromotePastCoveringMergeTarget)
{
    /// Pending covering merge [1, 10] replaces our local [1, 5] via
    /// `ActiveDataPartSet` covering semantics — `isCovered(5)` true, block.
    auto promoter = makePromoter({}, {{1, 10}});
    ASSERT_FALSE(promoter.canPromote(5, 11));
}
