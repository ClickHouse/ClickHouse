#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/SetVariants.h>

using namespace DB;

/// `HashMethodKeysFixed` batch-packs the keys of the whole column only when it is told it will be asked
/// about the whole column, and packs per row otherwise. Neither the choice nor the packed layout is
/// observable from SQL: there is no ProfileEvent for either, and the key column reordering that the
/// layout goes with is undone before results.
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

/// Widths 1, 8, 4, 2 in that clause order: 15 of the 16 bytes of a keys128 blob, and an order in which
/// no key lands at the same offset in clause order as it does in longest-first order.
struct MixedWidthKeyColumns
{
    Columns holders;
    ColumnRawPtrs columns;
    Sizes sizes;

    MixedWidthKeyColumns()
    {
        add<ColumnUInt8>(1);
        add<ColumnUInt64>(1000000);
        add<ColumnUInt32>(10000);
        add<ColumnUInt16>(1000);
    }

private:
    template <typename Column>
    void add(size_t base)
    {
        using Value = typename Column::ValueType;

        auto column = Column::create();
        auto & data = column->getData();
        data.resize(num_rows);
        /// Distinct and non-zero across rows and across columns, so a key byte left unpacked, or packed
        /// at the wrong offset, cannot compare equal by accident.
        for (size_t row = 0; row < num_rows; ++row)
            data[row] = static_cast<Value>(base + row);
        holders.emplace_back(std::move(column));
        columns.push_back(holders.back().get());
        sizes.push_back(sizeof(Value));
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
    ASSERT_FALSE(state.pack_keys_per_row);
    for (size_t row = 0; row < num_rows; ++row)
        ASSERT_EQ(state.getKeyHolder(row, arena), packFixed<UInt128>(row, keys.sizes.size(), keys.columns, keys.sizes))
            << "row " << row;
}

/// An in-order block that happens to be a single run passes its whole extent explicitly, and must not
/// lose the batching for saying so.
TEST(ColumnsHashingPreparedKeys, ExplicitWholeBlockRangeIsBatched)
{
    const KeyColumns keys(2);
    Arena arena;

    PreparedState state(keys.columns, keys.sizes, nullptr, {0, num_rows});

    ASSERT_EQ(state.prepared_keys.size(), num_rows);
    ASSERT_FALSE(state.pack_keys_per_row);
    for (size_t row = 0; row < num_rows; ++row)
        ASSERT_EQ(state.getKeyHolder(row, arena), packFixed<UInt128>(row, keys.sizes.size(), keys.columns, keys.sizes))
            << "row " << row;
}

/// A state built over a sub-range precomputes nothing and answers per row. The `{0, 29}` case is the one
/// that a range starting at row 0 would otherwise hide.
TEST(ColumnsHashingPreparedKeys, SubRangeIsNotBatched)
{
    const KeyColumns keys(2);
    Arena arena;

    PreparedState whole_block(keys.columns, keys.sizes, nullptr);

    for (const auto & range : {std::pair<size_t, size_t>{7, 29}, std::pair<size_t, size_t>{0, 29}})
    {
        PreparedState state(keys.columns, keys.sizes, nullptr, {range.first, range.second});

        const std::string range_name = std::to_string(range.first) + ".." + std::to_string(range.second);
        ASSERT_TRUE(state.prepared_keys.empty()) << "range " << range_name;
        ASSERT_TRUE(state.pack_keys_per_row) << "range " << range_name;
        for (size_t row = range.first; row < range.second; ++row)
            ASSERT_EQ(state.getKeyHolder(row, arena), whole_block.getKeyHolder(row, arena))
                << "range " << range_name << " row " << row;
    }
}

/// Separates "starts at row 0" from "covers the column".
TEST(ColumnsHashingPreparedKeys, SuffixSubRangeIsNotBatched)
{
    constexpr size_t range_begin = num_rows - 3;

    const KeyColumns keys(2);
    Arena arena;

    PreparedState whole_block(keys.columns, keys.sizes, nullptr);
    PreparedState state(keys.columns, keys.sizes, nullptr, {range_begin, num_rows});

    ASSERT_TRUE(state.prepared_keys.empty());
    ASSERT_TRUE(state.pack_keys_per_row);
    for (size_t row = range_begin; row < num_rows; ++row)
        ASSERT_EQ(state.getKeyHolder(row, arena), whole_block.getKeyHolder(row, arena)) << "row " << row;
}

/// The per-row packer has to reproduce the batch layout byte for byte: `shuffleKeyColumns` reorders the
/// output key columns into longest-first order for every `usePreparedKeys` key set, batched or not, so a
/// blob packed in clause order would be read back as a different key.
TEST(ColumnsHashingPreparedKeys, MixedWidthSubRangeMatchesBatchedLayout)
{
    constexpr size_t range_begin = 5;
    constexpr size_t range_end = 40;

    const MixedWidthKeyColumns keys;
    Arena arena;

    PreparedState batched(keys.columns, keys.sizes, nullptr);
    PreparedState ranged(keys.columns, keys.sizes, nullptr, {range_begin, range_end});

    ASSERT_EQ(batched.prepared_keys.size(), num_rows);
    ASSERT_TRUE(ranged.prepared_keys.empty());

    for (size_t row = range_begin; row < range_end; ++row)
    {
        ASSERT_EQ(ranged.getKeyHolder(row, arena), batched.getKeyHolder(row, arena)) << "row " << row;
        /// Clause order and longest-first order disagree for this key set, so this is what proves the
        /// assertion above compares layouts rather than coinciding on equal key widths.
        ASSERT_NE(batched.getKeyHolder(row, arena), packFixed<UInt128>(row, keys.sizes.size(), keys.columns, keys.sizes))
            << "row " << row;
    }
}

/// The discriminator: without it the cases above could pass on a constant.
TEST(ColumnsHashingPreparedKeys, WiderKeyIsNotBatched)
{
    const KeyColumns keys(3);

    UnpreparedState state(keys.columns, keys.sizes, nullptr);

    EXPECT_TRUE(state.prepared_keys.empty());
    EXPECT_FALSE(state.pack_keys_per_row);
}
