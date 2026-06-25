#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeCommittingBlock.h>

using namespace DB;

namespace
{
struct BlockSpec
{
    CommittingBlock::Op op;
    Int64 number;
    String partition_id;
};

CommittingBlocksSet makeBlocks(std::initializer_list<BlockSpec> specs)
{
    CommittingBlocksSet blocks;
    for (const auto & s : specs)
        blocks.emplace(s.op, s.number, s.partition_id);
    return blocks;
}

constexpr auto NewPart = CommittingBlock::Op::NewPart;
constexpr auto Mutation = CommittingBlock::Op::Mutation;
}

/// Reconstructs the real CI incident (STID 4462-571d): a merge over inputs ending at block 18 and
/// resuming at block 25 would span blocks 19..24, which a concurrent MOVE PARTITION is committing
/// into the same partition. Spanning that gap produces output 1_14_25_1 that intersects the gap
/// parts' own merge output 1_20_24_1.
TEST(CommittingBlockGap, RejectsMergeSpanningCommittingGap)
{
    auto committing = makeBlocks({
        {NewPart, 20, "1"}, {NewPart, 21, "1"}, {NewPart, 22, "1"}, {NewPart, 23, "1"}, {NewPart, 24, "1"}});

    EXPECT_TRUE(hasCommittingBlockInGap(committing, /*partition_id=*/"1", /*left_max_block=*/18, /*right_min_block=*/25));
}

TEST(CommittingBlockGap, AllowsMergeOfAdjacentParts)
{
    auto committing = makeBlocks({{NewPart, 20, "1"}});

    /// Adjacent parts (no gap) are always allowed.
    EXPECT_FALSE(hasCommittingBlockInGap(committing, "1", /*left_max_block=*/18, /*right_min_block=*/19));
}

TEST(CommittingBlockGap, AllowsMergeWhenGapHasNoCommittingBlocks)
{
    EXPECT_FALSE(hasCommittingBlockInGap(makeBlocks({{NewPart, 100, "1"}}), "1", 18, 25));
    EXPECT_FALSE(hasCommittingBlockInGap(/*committing_blocks=*/{}, "1", 18, 25));
}

TEST(CommittingBlockGap, IgnoresCommittingBlocksOutsideGap)
{
    /// Blocks at or below left.max_block, or at or above right.min_block, are not in the gap.
    auto committing = makeBlocks({{NewPart, 18, "1"}, {NewPart, 25, "1"}, {NewPart, 26, "1"}});

    EXPECT_FALSE(hasCommittingBlockInGap(committing, "1", 18, 25));
}

/// Block numbers are global across partitions. A block committing in another partition that happens
/// to fall numerically inside this partition's gap must NOT suppress the merge, otherwise busy
/// multi-partition tables would stop merging.
TEST(CommittingBlockGap, IgnoresCommittingBlocksFromOtherPartition)
{
    auto committing = makeBlocks({{NewPart, 22, "other"}});

    EXPECT_FALSE(hasCommittingBlockInGap(committing, "1", 18, 25));
    EXPECT_TRUE(hasCommittingBlockInGap(committing, "other", 18, 25));
}

/// Only NewPart blocks create an active part that fills a gap. Mutation/Update blocks consume the
/// same global counter as version bumps and must not suppress a merge.
TEST(CommittingBlockGap, IgnoresNonNewPartBlocks)
{
    auto committing = makeBlocks({{Mutation, 22, "1"}});

    EXPECT_FALSE(hasCommittingBlockInGap(committing, "1", 18, 25));
}

TEST(CommittingBlockGap, DetectsBoundaryBlocksInsideGap)
{
    /// One past left and one before right are both inside the gap.
    EXPECT_TRUE(hasCommittingBlockInGap(makeBlocks({{NewPart, 19, "1"}}), "1", 18, 25));
    EXPECT_TRUE(hasCommittingBlockInGap(makeBlocks({{NewPart, 24, "1"}}), "1", 18, 25));
}
