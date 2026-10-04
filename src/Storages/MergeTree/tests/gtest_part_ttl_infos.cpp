#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>


using namespace DB;

/// A patched merge restores the pre-patch GROUP BY entries over whatever the recalculation step
/// wrote, which leaves the bounds pointing at the throwaway values: `updatePartMinMaxTTL` only
/// accumulates, and it skips finished entries outright, so replaying it over the restored map
/// cannot undo them.
TEST(PartTTLInfos, RecalculateDropsBoundsOfReplacedEntries)
{
    MergeTreeDataPartTTLInfos infos;
    /// Finished, as the preserved GROUP BY entries are: `updatePartMinMaxTTL` would ignore this.
    infos.group_by_ttl["g"] = {.min = 300, .max = 400, .ttl_finished = true};

    /// Stale bounds, as if a throwaway recalculation had folded a wider range in.
    infos.part_min_ttl = 100;
    infos.part_max_ttl = 900;

    infos.recalculatePartMinMaxTTL();

    /// Rebuilt from the map, so the throwaway range is gone and the finished entry still counts.
    EXPECT_EQ(infos.part_min_ttl, 300);
    EXPECT_EQ(infos.part_max_ttl, 400);
}

/// The families that feed the bounds are the ones `update` folds in: recompression and moves do
/// not select parts by them and must stay out, or a part looks due for work it does not have.
TEST(PartTTLInfos, RecalculateUsesOnlyTheSelectingFamilies)
{
    MergeTreeDataPartTTLInfos infos;
    infos.columns_ttl["c"] = {.min = 500, .max = 600, .ttl_finished = false};
    infos.rows_where_ttl["w"] = {.min = 400, .max = 700, .ttl_finished = false};
    infos.table_ttl = {.min = 450, .max = 650, .ttl_finished = false};
    infos.recompression_ttl["r"] = {.min = 1, .max = 10000, .ttl_finished = false};
    infos.moves_ttl["m"] = {.min = 2, .max = 20000, .ttl_finished = false};

    infos.recalculatePartMinMaxTTL();

    EXPECT_EQ(infos.part_min_ttl, 400);
    EXPECT_EQ(infos.part_max_ttl, 700);
}

/// With nothing to select on, the bounds are cleared rather than left at their previous values.
TEST(PartTTLInfos, RecalculateClearsBoundsWhenNothingSelects)
{
    MergeTreeDataPartTTLInfos infos;
    infos.recompression_ttl["r"] = {.min = 1, .max = 10000, .ttl_finished = false};
    infos.part_min_ttl = 100;
    infos.part_max_ttl = 900;

    infos.recalculatePartMinMaxTTL();

    EXPECT_EQ(infos.part_min_ttl, 0);
    EXPECT_EQ(infos.part_max_ttl, 0);
}
