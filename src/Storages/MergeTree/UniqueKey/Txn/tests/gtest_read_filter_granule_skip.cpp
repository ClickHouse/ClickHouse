/// Read-path: delete-bitmap granule-skip + row-level filter against a
/// snapshot-anchored bitmap.
///
/// The production read path
/// (`ReadFromMergeTree::selectRangesToRead` -> `MergeTreeSelectProcessor::
/// readCurrentTask`) consumes the bitmap returned by a partition's
/// `QuerySnapshot::bitmap_at(part)` two ways:
///   1. Granule skip — at index-analysis time, a mark whose every row is
///      dead (`bitmap.rangeCardinality(start, start+rows) == rows`) is
///      dropped from the part's `MarkRanges`.
///   2. Row-level filter — at scan time, a surviving row is dropped iff
///      `bitmap.contains(part_offset)`.
///
/// These tests plant versioned bitmaps in a fake `IBitmapStore`, take a real
/// `PartitionTxnController::takeQuerySnapshot()` (so the csn-gating + pin RAII is
/// the real code), then drive the SAME production primitives the read path
/// uses — `selectLiveMarkRanges` and `buildDeleteBitmapFilter` from
/// `UniqueKeyReadFilter` — so the test arithmetic cannot drift from the code
/// that actually runs.
#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyReadFilter.h>
#include <Storages/MergeTree/UniqueKey/Txn/tests/gtest_txn_fakes.h>

#include <Common/PODArray.h>

#include <vector>

using namespace DB;
using namespace DB::UniqueKeyTxn;
using namespace DB::UniqueKeyTxn::tests;

namespace
{

/// `rowFilter` calls the production `buildDeleteBitmapFilter` and materializes
/// the surviving offsets from its mask, so the row-filter assertions exercise
/// the real masking (full-UInt64, no narrowing) rather than a copy. The
/// granule-skip tests call the production `selectLiveMarkRanges` directly.
std::vector<UInt64> rowFilter(const std::vector<UInt64> & part_offsets, const DeleteBitmap & bitmap)
{
    PaddedPODArray<UInt8> filter;
    buildDeleteBitmapFilter(part_offsets.data(), part_offsets.size(), bitmap, filter);
    std::vector<UInt64> kept;
    for (size_t i = 0; i < part_offsets.size(); ++i)
        if (filter[i])
            kept.push_back(part_offsets[i]);
    return kept;
}

}

/// Granule-skip shape over the three corners that change the kept-range output:
///   * a fully-dead interior mark is excised, splitting the run (partial /
///     alive marks survive);
///   * every mark dead -> nothing kept (the part is dropped from
///     `parts_with_ranges` by the production `erase_if`);
///   * an empty bitmap -> nothing skipped, full range kept (zero-overhead fast
///     path).
TEST(ReadFilterGranuleSkip, FullyDeadGranuleSkippedPartialKept)
{
    /// Mixed: 3 marks of 4 rows each (12 rows), mark 1 (rows 4..7) fully dead,
    /// mark 2 partial. Mark 0 (alive) and mark 2 survive; mark 1 is excised, so
    /// the run splits into two ranges [0,1) and [2,3).
    {
        MergeTreeIndexGranularityConstant gran(/*constant*/ 4, /*last*/ 4, /*num_marks_without_final*/ 3, /*has_final*/ false);
        auto bitmap = makeBitmap({4, 5, 6, 7, /*partial on mark 2*/ 8});
        auto res = selectLiveMarkRanges(MarkRanges{MarkRange{0, 3}}, gran, *bitmap);
        EXPECT_EQ(res.skipped, 1u);
        ASSERT_EQ(res.kept.size(), 2u);
        EXPECT_EQ(res.kept[0].begin, 0u); EXPECT_EQ(res.kept[0].end, 1u);
        EXPECT_EQ(res.kept[1].begin, 2u); EXPECT_EQ(res.kept[1].end, 3u);
    }

    /// Every granule dead -> all marks skipped, no ranges kept.
    {
        MergeTreeIndexGranularityConstant gran(4, 4, 2, false);
        auto bitmap = makeBitmap({0, 1, 2, 3, 4, 5, 6, 7});
        auto res = selectLiveMarkRanges(MarkRanges{MarkRange{0, 2}}, gran, *bitmap);
        EXPECT_EQ(res.skipped, 2u);
        EXPECT_TRUE(res.kept.empty());
    }

    /// Empty bitmap -> nothing skipped, full range kept (zero-overhead fast path).
    {
        MergeTreeIndexGranularityConstant gran(4, 4, 3, false);
        DeleteBitmap empty;
        auto res = selectLiveMarkRanges(MarkRanges{MarkRange{0, 3}}, gran, empty);
        EXPECT_EQ(res.skipped, 0u);
        ASSERT_EQ(res.kept.size(), 1u);
        EXPECT_EQ(res.kept[0].begin, 0u); EXPECT_EQ(res.kept[0].end, 3u);
    }
}

/// Row-level filter keeps exactly the live offsets within a surviving granule.
TEST(ReadFilterRowFilter, KeepsLiveOffsets)
{
    auto bitmap = makeBitmap({1, 3});
    std::vector<UInt64> offsets{0, 1, 2, 3};
    EXPECT_EQ(rowFilter(offsets, *bitmap), (std::vector<UInt64>{0, 2}));
}

TEST(ReadFilterRowFilter, AllRowsDeadYieldsEmpty)
{
    auto bitmap = makeBitmap({0, 1, 2});
    EXPECT_TRUE(rowFilter({0, 1, 2}, *bitmap).empty());
}

/// Regression: `_part_offset` above UInt32 must not be narrowed before the
/// bitmap lookup. The dead row sits at `2^32 + 5`; its low 32 bits are 5. A
/// truncating filter would (a) miss the real dead row at `2^32 + 5` and
/// (b) wrongly drop the live row at offset 5. Both must be correct.
TEST(ReadFilterRowFilter, OffsetAboveUInt32NotTruncated)
{
    constexpr UInt64 base = UInt64(1) << 32;
    auto bitmap = makeBitmap({base + 5});
    /// Offset 5 is live (only base+5 is dead); base+5 is dead.
    std::vector<UInt64> offsets{5, base + 4, base + 5, base + 6};
    EXPECT_EQ(rowFilter(offsets, *bitmap), (std::vector<UInt64>{5, base + 4, base + 6}));
    EXPECT_TRUE(bitmap->contains(base + 5));
    EXPECT_FALSE(bitmap->contains(5));
}

/// The returned `kept` count is what production passes to `column->filter` and
/// the `kept == 0` empty-chunk branch — assert it directly alongside the mask.
TEST(ReadFilterRowFilter, ReturnsKeptCountAndMask)
{
    auto bitmap = makeBitmap({1, 3});
    std::vector<UInt64> offsets{0, 1, 2, 3, 4};
    PaddedPODArray<UInt8> filter;
    const size_t kept = buildDeleteBitmapFilter(offsets.data(), offsets.size(), *bitmap, filter);
    EXPECT_EQ(kept, 3u); /// 0,2,4 live; 1,3 dead
    ASSERT_EQ(filter.size(), offsets.size());
    EXPECT_EQ(filter[0], 1u); EXPECT_EQ(filter[1], 0u); EXPECT_EQ(filter[2], 1u);
    EXPECT_EQ(filter[3], 0u); EXPECT_EQ(filter[4], 1u);
}

/// Short last data mark: its size must come from `getMarkRows`, not the
/// constant granularity. `last_mark_granularity = 2` makes mark 1 cover rows
/// [4,6); it is fully dead only when BOTH rows die. Using the constant (4)
/// would mis-judge `rangeCardinality([4,8)) == 2 != 4` and wrongly keep it.
TEST(ReadFilterGranuleSkip, ShortLastMarkSizedByGetMarkRows)
{
    /// mark 0 = 4 rows [0,4), mark 1 (last data mark) = 2 rows [4,6).
    MergeTreeIndexGranularityConstant gran(/*constant*/ 4, /*last*/ 2, /*num_marks_without_final*/ 2, /*has_final*/ true);
    MarkRanges all{MarkRange{0, 2}}; /// the two data marks (final sentinel excluded)

    /// Both rows of the 2-row mark dead -> mark 1 skipped.
    auto full = selectLiveMarkRanges(all, gran, *makeBitmap({4, 5}));
    EXPECT_EQ(full.skipped, 1u);
    ASSERT_EQ(full.kept.size(), 1u);
    EXPECT_EQ(full.kept[0].begin, 0u); EXPECT_EQ(full.kept[0].end, 1u);

    /// Only one of the two dead -> mark 1 survives, nothing skipped.
    auto partial = selectLiveMarkRanges(all, gran, *makeBitmap({4}));
    EXPECT_EQ(partial.skipped, 0u);
    ASSERT_EQ(partial.kept.size(), 1u);
    EXPECT_EQ(partial.kept[0].begin, 0u); EXPECT_EQ(partial.kept[0].end, 2u);
}

/// Zero-row granule is never "fully dead" (the `granule_rows > 0` guard), so it
/// is kept even when the preceding granule dies.
TEST(ReadFilterGranuleSkip, ZeroRowMarkNeverSkipped)
{
    /// mark 0 = 4 rows [0,4), mark 1 (last data mark) = 0 rows.
    MergeTreeIndexGranularityConstant gran(/*constant*/ 4, /*last*/ 0, /*num_marks_without_final*/ 2, /*has_final*/ true);
    MarkRanges all{MarkRange{0, 2}};

    auto res = selectLiveMarkRanges(all, gran, *makeBitmap({0, 1, 2, 3})); /// mark 0 fully dead
    EXPECT_EQ(res.skipped, 1u);
    ASSERT_EQ(res.kept.size(), 1u);
    EXPECT_EQ(res.kept[0].begin, 1u); EXPECT_EQ(res.kept[0].end, 2u); /// 0-row mark 1 kept by the guard
}

/// The bitmap the read path filters with comes from the real snapshot at the
/// planted csn — exercises csn-gating + the granule-skip together.
TEST(ReadFilterGranuleSkip, SnapshotCsnGatedBitmapDrivesSkip)
{
    auto fx = makeFixture();
    fx.coord->setCurrent(5);
    /// At csn=3, mark 0 (rows 0..3) is fully dead. At csn=7 (above snapshot),
    /// mark 1 would also die — must NOT be seen by a csn=5 snapshot.
    fx.store->seed("all_1_1_0", 3, makeBitmap({0, 1, 2, 3}));
    fx.store->seed("all_1_1_0", 7, makeBitmap({0, 1, 2, 3, 4, 5, 6, 7}));

    auto snap = fx.state->takeQuerySnapshot();
    ASSERT_TRUE(snap->bitmap_at);
    EXPECT_EQ(snap->csn, 5u);
    EXPECT_EQ(fx.pins->total(), 1u); /// pin held while snapshot lives

    auto bitmap = snap->bitmap_at("all_1_1_0");
    ASSERT_NE(bitmap, nullptr);

    MergeTreeIndexGranularityConstant gran(4, 4, 2, false);
    auto res = selectLiveMarkRanges(MarkRanges{MarkRange{0, 2}}, gran, *bitmap);
    /// Only mark 0 dead at csn=5; mark 1 survives (its death lives at csn=7).
    EXPECT_EQ(res.skipped, 1u);
    ASSERT_EQ(res.kept.size(), 1u);
    EXPECT_EQ(res.kept[0].begin, 1u); EXPECT_EQ(res.kept[0].end, 2u);
}
