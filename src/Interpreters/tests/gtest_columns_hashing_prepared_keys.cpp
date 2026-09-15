#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/SetVariants.h>

using namespace DB;

/// `HashMethodKeysFixed` batch-packs the keys of the rows it is told about and stores them indexed by
/// absolute row number. Neither half is observable from SQL: the batching predicate has no
/// ProfileEvent and no system table, and the column reordering it enables is undone before results.
namespace
{

/// Two 8-byte non-nullable keys pack into 16 bytes, the widest key `usePreparedKeys` accepts. This is
/// the state DISTINCT in order builds per equal-range of the sorting prefix (`SetVariants.h` keys128).
using PreparedState = SetMethodKeysFixed<ClearableHashSet<UInt128, UInt128HashCRC32>>::State;
/// Three of them need 24 bytes, which leaves that class.
using UnpreparedState = SetMethodKeysFixed<ClearableHashSet<UInt256, UInt256HashCRC32>>::State;

constexpr size_t num_rows = 64;

struct KeyColumns
{
    Columns holders;
    ColumnRawPtrs columns;
    Sizes sizes;

    explicit KeyColumns(size_t num_keys)
    {
        for (size_t key = 0; key < num_keys; ++key)
        {
            auto column = ColumnUInt64::create();
            auto & data = column->getData();
            data.resize(num_rows);
            /// No value repeats across rows or columns, and none is zero, so a row that was not
            /// packed cannot accidentally compare equal to one that was.
            for (size_t row = 0; row < num_rows; ++row)
                data[row] = (row + 1) * (2 * key + 3);
            holders.emplace_back(std::move(column));
            columns.push_back(holders.back().get());
            sizes.push_back(sizeof(UInt64));
        }
    }
};

}

/// The whole-block callers (every fixed-key `GROUP BY` outside the in-order path) must keep batching.
TEST(ColumnsHashingPreparedKeys, WholeBlockIsBatched)
{
    const KeyColumns keys(2);
    Arena arena;

    PreparedState state(keys.columns, keys.sizes, nullptr);

    ASSERT_EQ(state.prepared_keys.size(), num_rows);
    for (size_t row = 0; row < num_rows; ++row)
        ASSERT_EQ(state.getKeyHolder(row, arena), packFixed<UInt128>(row, keys.sizes.size(), keys.columns, keys.sizes))
            << "row " << row;
}

/// A ranged caller gets its range packed, addressed by absolute row number. The oracle is the per-row
/// packer, which does not share the batched code path.
TEST(ColumnsHashingPreparedKeys, RangeIsBatchedAtAbsoluteRowIndexes)
{
    constexpr size_t range_begin = 7;
    constexpr size_t range_end = 29;

    const KeyColumns keys(2);
    Arena arena;

    PreparedState state(keys.columns, keys.sizes, nullptr, {range_begin, range_end});

    ASSERT_EQ(state.prepared_keys.size(), range_end);
    for (size_t row = range_begin; row < range_end; ++row)
        ASSERT_EQ(state.getKeyHolder(row, arena), packFixed<UInt128>(row, keys.sizes.size(), keys.columns, keys.sizes))
            << "row " << row;
}

/// The discriminator: without it the two cases above could pass on a constant.
TEST(ColumnsHashingPreparedKeys, WiderKeyIsNotBatched)
{
    const KeyColumns keys(3);

    UnpreparedState state(keys.columns, keys.sizes, nullptr);

    EXPECT_TRUE(state.prepared_keys.empty());
}
