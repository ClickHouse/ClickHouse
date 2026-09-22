#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
/// Any flagged combination will do; the per-row path does not depend on which.
constexpr auto KIND = JoinKind::Full;
constexpr auto STRICTNESS = JoinStrictness::All;
constexpr auto MAPS_KIND = JoinMapsKind::All;
}

TEST(JoinUsedFlags, AllOffsetFlagsSetCountsOccupiedKeys)
{
    JoinStuff::JoinUsedFlags flags;
    flags.per_offset_flags = JoinStuff::JoinUsedFlags::UsedFlagsForColumns(8);
    flags.setUnsetOffsetCount(3);

    EXPECT_FALSE(flags.allOffsetFlagsSet());

    flags.setUsed<true, false>(0, 0, 1);
    EXPECT_FALSE(flags.allOffsetFlagsSet());

    /// Duplicate mark of the same offset must not underflow the unused-key count.
    flags.setUsed<true, false>(0, 0, 1);
    EXPECT_FALSE(flags.allOffsetFlagsSet());

    flags.setUsed<true, false>(0, 0, 2);
    const bool first_once = flags.setUsedOnce<true, false>(0, 0, 4);
    EXPECT_TRUE(first_once);
    EXPECT_TRUE(flags.allOffsetFlagsSet());

    const bool second_once = flags.setUsedOnce<true, false>(0, 0, 4);
    EXPECT_FALSE(second_once);
    EXPECT_TRUE(flags.allOffsetFlagsSet());
}

TEST(JoinUsedFlags, AllOffsetFlagsSetEmptyCount)
{
    JoinStuff::JoinUsedFlags flags;
    EXPECT_TRUE(flags.allOffsetFlagsSet());
    flags.setUnsetOffsetCount(0);
    EXPECT_TRUE(flags.allOffsetFlagsSet());
}

TEST(JoinUsedFlags, PerRowFlagsMarkRowsOutsideSelectorAsUsed)
{
    JoinStuff::JoinUsedFlags flags;

    /// Rows outside the selector must come out pre-marked, or RIGHT/FULL emits them twice.
    auto indexes = ScatteredBlock::Selector::Indexes::create();
    indexes->insertValue(1);
    indexes->insertValue(3);
    flags.reinit<KIND, STRICTNESS, MAPS_KIND>(/*block_no=*/0, /*rows=*/4, ScatteredBlock::Selector(std::move(indexes)));

    EXPECT_TRUE(flags.getUsedSafe(0, 0));
    EXPECT_FALSE(flags.getUsedSafe(0, 1));
    EXPECT_TRUE(flags.getUsedSafe(0, 2));
    EXPECT_FALSE(flags.getUsedSafe(0, 3));
}
