/// Read-path: delete-bitmap row-level filter via `DeleteBitmap::buildKeepFilter`
/// (the production `MergeTreeSelectProcessor` row mask).
///
/// Granule-skip (the mark-range selection) is file-local to
/// `UniqueKeyReadFilter.cpp` and covered end-to-end by the stateless test
/// `04155_unique_key_delete_partitions_predicates.sql` (section 2:
/// index_granularity=4, 8 rows -> 2 granules, the first granule fully deleted).
#include <gtest/gtest.h>

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/Txn/tests/gtest_txn_fakes.h>

#include <Common/PODArray.h>

#include <vector>

using namespace DB;
using namespace DB::UniqueKeyTxn;
using namespace DB::UniqueKeyTxn::tests;

namespace
{

/// `rowFilter` calls the production `DeleteBitmap::buildKeepFilter` and
/// materializes the surviving offsets from its mask, so the assertions exercise
/// the real masking (full-UInt64, no narrowing) rather than a copy.
std::vector<UInt64> rowFilter(const std::vector<UInt64> & part_offsets, const DeleteBitmap & bitmap)
{
    PaddedPODArray<UInt8> filter(part_offsets.size());
    bitmap.buildKeepFilter(part_offsets.data(), part_offsets.size(), filter.data());
    std::vector<UInt64> kept;
    for (size_t i = 0; i < part_offsets.size(); ++i)
        if (filter[i])
            kept.push_back(part_offsets[i]);
    return kept;
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
    PaddedPODArray<UInt8> filter(offsets.size());
    const size_t kept = bitmap->buildKeepFilter(offsets.data(), offsets.size(), filter.data());
    EXPECT_EQ(kept, 3u); /// 0,2,4 live; 1,3 dead
    ASSERT_EQ(filter.size(), offsets.size());
    EXPECT_EQ(filter[0], 1u); EXPECT_EQ(filter[1], 0u); EXPECT_EQ(filter[2], 1u);
    EXPECT_EQ(filter[3], 0u); EXPECT_EQ(filter[4], 1u);
}
