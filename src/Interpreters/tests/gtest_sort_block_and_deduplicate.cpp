#include <gtest/gtest.h>

#include <numeric>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/sortBlock.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

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

std::vector<String> stringValues(const Block & block, const String & name)
{
    const auto & column = block.getByName(name).column;
    std::vector<String> result;
    for (size_t row = 0; row < column->size(); ++row)
        result.emplace_back(column->getDataAt(row));
    return result;
}

Block blockWithSharedIndexes(ColumnWithTypeAndName key)
{
    const auto indexes = numbers("indexes", {2, 0, 2, 1, 0}).column;
    auto left = strings("left", {"zero", "one", "two"});
    auto right = strings("right", {"0", "1", "2"});
    left.column = ColumnReplicated::create(left.column, indexes);
    right.column = ColumnReplicated::create(right.column, indexes);
    return {std::move(key), std::move(left), std::move(right), arrivals(5)};
}

void checkSharedIndexes(const Block & block)
{
    const auto * left = typeid_cast<const ColumnReplicated *>(block.getByName("left").column.get());
    const auto * right = typeid_cast<const ColumnReplicated *>(block.getByName("right").column.get());
    ASSERT_NE(left, nullptr);
    ASSERT_NE(right, nullptr);
    EXPECT_EQ(left->getIndexesColumn(), right->getIndexesColumn());
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

TEST(SortBlockAndDeduplicate, CollationEquivalentStringsKeepTheFirstPayload)
{
    /// Long equal ranges exercise collation comparisons beyond the initial linear probe.
    std::vector<String> keys;
    for (size_t i = 0; i < 10; ++i)
        keys.insert(keys.end(), {"b", "A", "a", "B"});
    Block block{strings("k", keys), arrivals(keys.size())};
    SortDescription description;
    description.emplace_back("k", 1, 1, std::make_shared<Collator>("en-u-ks-level2"));

    sortBlockAndDeduplicate(block, description, IColumn::PermutationSortStability::Stable);

    EXPECT_EQ(stringValues(block, "k"), (std::vector<String>{"A", "b"}));
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{1, 0}));
}

TEST(SortBlockAndDeduplicate, MultipleKeysUseCollationForEqualRanges)
{
    Block block{numbers("n", {2, 1, 1, 2, 1, 2}), strings("s", {"a", "b", "B", "A", "a", "b"}), arrivals(6)};
    SortDescription description;
    description.emplace_back("n", 1, 1);
    description.emplace_back("s", 1, 1, std::make_shared<Collator>("en-u-ks-level2"));

    sortBlockAndDeduplicate(block, description, IColumn::PermutationSortStability::Stable);

    EXPECT_EQ(values(block, "n"), (std::vector<UInt64>{1, 1, 2, 2}));
    EXPECT_EQ(stringValues(block, "s"), (std::vector<String>{"a", "b", "a", "b"}));
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{4, 1, 0, 5}));
}

TEST(SortBlockAndDeduplicate, CompactionPreservesSharedIndexes)
{
    Block block = blockWithSharedIndexes(numbers("k", {3, 1, 3, 2, 1}));
    checkSharedIndexes(block);

    sortBlockAndDeduplicate(block, ascending({"left"}), IColumn::PermutationSortStability::Stable);

    EXPECT_EQ(stringValues(block, "left"), (std::vector<String>{"one", "two", "zero"}));
    EXPECT_EQ(stringValues(block, "right"), (std::vector<String>{"1", "2", "0"}));
    EXPECT_EQ(values(block, "k"), (std::vector<UInt64>{2, 3, 1}));
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{3, 0, 1}));
    checkSharedIndexes(block);
}

TEST(SortBlockAndDeduplicate, ConstantSortKeyPreservesSharedIndexes)
{
    auto type = std::make_shared<DataTypeUInt64>();
    Block block = blockWithSharedIndexes({ColumnConst::create(ColumnUInt64::create(1, 7), 5), type, "k"});
    checkSharedIndexes(block);

    sortBlockAndDeduplicate(block, ascending({"k"}), IColumn::PermutationSortStability::Stable);

    EXPECT_EQ(stringValues(block, "left"), (std::vector<String>{"two"}));
    EXPECT_EQ(stringValues(block, "right"), (std::vector<String>{"2"}));
    EXPECT_EQ(values(block, "arrival"), (std::vector<UInt64>{0}));
    checkSharedIndexes(block);
}
