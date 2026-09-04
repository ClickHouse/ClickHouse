#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
/// Any flagged combination will do; the per-row path does not depend on which.
constexpr auto KIND = JoinKind::Full;
constexpr auto STRICTNESS = JoinStrictness::All;
constexpr bool PREFER_MAPS_ALL = true;
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

/// Concurrent `reinit` from different workers must not share a mutated container.
TEST(JoinUsedFlags, PendingPerRowFlagsMergeAcrossWorkers)
{
    JoinStuff::JoinUsedFlags flags;
    flags.setPendingFlagWorkers(/*num_workers=*/2, /*need_flags_=*/true);

    flags.reinit<KIND, STRICTNESS, PREFER_MAPS_ALL>(/*worker_id=*/0, /*block_no=*/0, /*rows=*/3, ScatteredBlock::Selector(3));
    flags.reinit<KIND, STRICTNESS, PREFER_MAPS_ALL>(/*worker_id=*/1, /*block_no=*/1, /*rows=*/2, ScatteredBlock::Selector(2));

    flags.finalizePerRowFlags(/*num_blocks=*/2);

    EXPECT_FALSE(flags.getUsedSafe(0, 0));
    EXPECT_FALSE(flags.getUsedSafe(0, 1));
    EXPECT_FALSE(flags.getUsedSafe(0, 2));
    EXPECT_FALSE(flags.getUsedSafe(1, 0));
    EXPECT_FALSE(flags.getUsedSafe(1, 1));

    /// A second finalize (e.g. if onBuildPhaseFinish ran twice) must be a no-op, not a re-throw.
    flags.finalizePerRowFlags(/*num_blocks=*/2);
    EXPECT_FALSE(flags.getUsedSafe(0, 0));
}

TEST(JoinUsedFlags, PendingPerRowFlagsMarksRowsOutsideSelectorAsUsed)
{
    JoinStuff::JoinUsedFlags flags;
    flags.setPendingFlagWorkers(/*num_workers=*/1);

    /// Rows outside the selector must come out pre-marked, or RIGHT/FULL emits them twice.
    auto indexes = ScatteredBlock::Selector::Indexes::create();
    indexes->insertValue(1);
    indexes->insertValue(3);
    flags.reinit<KIND, STRICTNESS, PREFER_MAPS_ALL>(
        /*worker_id=*/0, /*block_no=*/0, /*rows=*/4, ScatteredBlock::Selector(std::move(indexes)));

    flags.finalizePerRowFlags(/*num_blocks=*/1);

    EXPECT_TRUE(flags.getUsedSafe(0, 0));
    EXPECT_FALSE(flags.getUsedSafe(0, 1));
    EXPECT_TRUE(flags.getUsedSafe(0, 2));
    EXPECT_FALSE(flags.getUsedSafe(0, 3));
}

TEST(JoinUsedFlags, PendingPerRowFlagsDuplicateBlockNoAcrossWorkersThrows)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "LOGICAL_ERROR aborts in debug and sanitizer builds";
#else
    JoinStuff::JoinUsedFlags flags;
    flags.setPendingFlagWorkers(/*num_workers=*/2);

    /// `StoredColumnsIndex::add` assigns each `block_no` once; a duplicate must not overwrite.
    flags.reinit<KIND, STRICTNESS, PREFER_MAPS_ALL>(/*worker_id=*/0, /*block_no=*/5, /*rows=*/1, ScatteredBlock::Selector(1));
    flags.reinit<KIND, STRICTNESS, PREFER_MAPS_ALL>(/*worker_id=*/1, /*block_no=*/5, /*rows=*/1, ScatteredBlock::Selector(1));

    EXPECT_THROW(flags.finalizePerRowFlags(/*num_blocks=*/6), Exception);
#endif
}
