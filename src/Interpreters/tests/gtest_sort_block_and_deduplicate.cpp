#include <gtest/gtest.h>

#include <numeric>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/sortBlock.h>
#include <Common/assert_cast.h>

using namespace DB;

/// With stable sorting, `sortBlockAndDeduplicate` retains the first input row for each sort key.
/// The `arrival` payload records input positions so the tests can identify the retained rows.
namespace
{

ColumnWithTypeAndName numbers(const String & name, const std::vector<UInt64> & values)
{
    auto column = ColumnUInt64::create();
    for (const auto value : values)
        column->insertValue(value);
    return {std::move(column), std::make_shared<DataTypeUInt64>(), name};
}

ColumnWithTypeAndName strings(const String & name, const std::vector<String> & values)
{
    auto column = ColumnString::create();
    for (const auto & value : values)
        column->insertData(value.data(), value.size());
    return {std::move(column), std::make_shared<DataTypeString>(), name};
}

ColumnWithTypeAndName arrivals(size_t rows)
{
    std::vector<UInt64> values(rows);
    std::iota(values.begin(), values.end(), 0);
    return numbers("arrival", values);
}

std::vector<UInt64> values(const Block & block, const String & name)
{
    const auto & data = assert_cast<const ColumnUInt64 &>(*block.getByName(name).column).getData();
    return {data.begin(), data.end()};
}

SortDescription ascending(const std::vector<String> & names)
{
    SortDescription description;
    for (const auto & name : names)
        description.emplace_back(name, 1, 1);
    return description;
}

}

TEST(SortBlockAndDeduplicate, SingleColumnKeepsTheFirstReceivedRowOfEachKey)
{
    /// A single sort column takes the plain permutation path; the equal ranges are found from the sorted
    /// order.
    Block block{numbers("k", {3, 1, 3, 2, 1, 3}), arrivals(6)};
    sortBlockAndDeduplicate(block, ascending({"k"}), IColumn::PermutationSortStability::Stable);

    EXPECT_EQ(values(block, "k"), (std::vector<UInt64>{1, 2, 3}));
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{1, 3, 0}));
}

TEST(SortBlockAndDeduplicate, SeveralColumnsUseTheEqualRangesOfTheSort)
{
    /// Several sort columns take the range-refining path, whose final equal ranges are the duplicates.
    Block block{numbers("a", {1, 1, 2, 1, 1}), strings("b", {"x", "y", "x", "x", "y"}), arrivals(5)};
    sortBlockAndDeduplicate(block, ascending({"a", "b"}), IColumn::PermutationSortStability::Stable);

    EXPECT_EQ(values(block, "a"), (std::vector<UInt64>{1, 1, 2}));
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{0, 1, 2}));
}

TEST(SortBlockAndDeduplicate, RowsWithoutDuplicatesAreOnlySorted)
{
    Block block{numbers("k", {2, 1, 3}), arrivals(3)};
    sortBlockAndDeduplicate(block, ascending({"k"}), IColumn::PermutationSortStability::Stable);

    EXPECT_EQ(values(block, "k"), (std::vector<UInt64>{1, 2, 3}));
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{1, 0, 2}));
}

TEST(SortBlockAndDeduplicate, SortedRowsWithoutDuplicatesStayAsTheyAre)
{
    Block block{numbers("k", {1, 2, 3}), arrivals(3)};
    const auto * column_before = block.getByName("k").column.get();
    sortBlockAndDeduplicate(block, ascending({"k"}), IColumn::PermutationSortStability::Stable);

    EXPECT_EQ(block.getByName("k").column.get(), column_before);
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{0, 1, 2}));
}

TEST(SortBlockAndDeduplicate, ConstantSortColumnLeavesOneRow)
{
    auto type = std::make_shared<DataTypeUInt64>();
    Block block{{ColumnConst::create(ColumnUInt64::create(1, 7), 4), type, "k"}, arrivals(4)};
    sortBlockAndDeduplicate(block, ascending({"k"}), IColumn::PermutationSortStability::Stable);

    EXPECT_EQ(block.rows(), 1u);
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{0}));
}

TEST(SortBlockAndDeduplicate, DescendingDirection)
{
    Block block{numbers("k", {1, 3, 1, 3}), arrivals(4)};
    SortDescription description;
    description.emplace_back("k", -1, 1);
    sortBlockAndDeduplicate(block, description, IColumn::PermutationSortStability::Stable);

    EXPECT_EQ(values(block, "k"), (std::vector<UInt64>{3, 1}));
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{1, 0}));
}

TEST(SortBlockAndDeduplicate, NullsAreOneKey)
{
    auto nested = ColumnUInt64::create();
    auto null_map = ColumnUInt8::create();
    for (const auto [value, is_null] : std::vector<std::pair<UInt64, UInt8>>{{0, 1}, {1, 0}, {0, 1}, {1, 0}})
    {
        nested->insertValue(value);
        null_map->insertValue(is_null);
    }
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    Block block{{ColumnNullable::create(std::move(nested), std::move(null_map)), nullable_type, "k"}, arrivals(4)};
    sortBlockAndDeduplicate(block, ascending({"k"}), IColumn::PermutationSortStability::Stable);

    /// `NULL` values sort last (`nulls_direction` = 1); the first `NULL` row and the first 1 row survive.
    EXPECT_EQ(block.rows(), 2u);
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{1, 0}));
}

TEST(SortBlockAndDeduplicate, EmptyBlock)
{
    Block block{numbers("k", {}), arrivals(0)};
    sortBlockAndDeduplicate(block, ascending({"k"}), IColumn::PermutationSortStability::Stable);
    EXPECT_EQ(block.rows(), 0u);
}

TEST(SortBlockAndDeduplicate, EqualValuesKeepTheFirstPayload)
{
    Block block{numbers("k", {7, 7, 7, 7}), arrivals(4)};
    sortBlockAndDeduplicate(block, ascending({"k"}), IColumn::PermutationSortStability::Stable);

    EXPECT_EQ(values(block, "k"), (std::vector<UInt64>{7}));
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{0}));
}
