#include <gtest/gtest.h>

#include <limits>
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

/// With stable sorting, `sortBlockAndLimitGroupRows` retains the first `group_limit` input rows of each sort
/// key. The `arrival` payload records input positions so the tests can identify the retained rows. The tests
/// use the limit of one row of `DISTINCT` and the larger limit of `LIMIT BY`, with a key that exceeds both.
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

/// `left` and `right` select "zero", "one" or "two" through the same `indexes`, while `k` is constant.
Block blockWithSharedIndexes(const std::vector<UInt64> & indexes)
{
    const auto indexes_column = numbers("indexes", indexes).column;
    auto left = strings("left", {"zero", "one", "two"});
    auto right = strings("right", {"0", "1", "2"});
    left.column = ColumnReplicated::create(left.column, indexes_column);
    right.column = ColumnReplicated::create(right.column, indexes_column);
    ColumnWithTypeAndName key{
        ColumnConst::create(ColumnUInt64::create(1, 7), indexes.size()), std::make_shared<DataTypeUInt64>(), "k"};
    return {std::move(key), std::move(left), std::move(right), arrivals(indexes.size())};
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

TEST(SortBlockAndLimitGroupRows, SingleColumnKeepsTheFirstRowsOfEachKey)
{
    /// A single sort column takes the plain permutation path; the equal ranges are found from the sorted
    /// order.
    struct Case
    {
        size_t group_limit;
        std::vector<UInt64> keys;
        std::vector<UInt64> arrival;
    };
    const std::vector<Case> cases{{1, {1, 2, 3}, {1, 3, 0}}, {2, {1, 1, 2, 3, 3}, {1, 4, 3, 0, 2}}};
    for (const auto & [group_limit, keys, arrival] : cases)
    {
        SCOPED_TRACE(group_limit);
        Block block{numbers("k", {3, 1, 3, 2, 1, 3}), arrivals(6)};
        sortBlockAndLimitGroupRows(block, ascending({"k"}), group_limit, IColumn::PermutationSortStability::Stable);

        EXPECT_EQ(values(block, "k"), keys);
        EXPECT_EQ(values(block, "arrival"), arrival);
    }
}

TEST(SortBlockAndLimitGroupRows, SeveralColumnsUseTheEqualRangesOfTheSort)
{
    /// Several sort columns take the range-refining path, whose final equal ranges are the groups.
    struct Case
    {
        size_t group_limit;
        std::vector<UInt64> a;
        std::vector<UInt64> arrival;
    };
    const std::vector<Case> cases{{1, {1, 1, 2}, {0, 1, 2}}, {2, {1, 1, 1, 1, 2}, {0, 3, 1, 4, 2}}};
    for (const auto & [group_limit, a, arrival] : cases)
    {
        SCOPED_TRACE(group_limit);
        Block block{numbers("a", {1, 1, 2, 1, 1, 1}), strings("b", {"x", "y", "x", "x", "y", "x"}), arrivals(6)};
        sortBlockAndLimitGroupRows(block, ascending({"a", "b"}), group_limit, IColumn::PermutationSortStability::Stable);

        EXPECT_EQ(values(block, "a"), a);
        EXPECT_EQ(values(block, "arrival"), arrival);
    }
}

TEST(SortBlockAndLimitGroupRows, RowsWithinTheLimitAreOnlySorted)
{
    struct Case
    {
        size_t group_limit;
        std::vector<UInt64> input_keys;
        std::vector<UInt64> sorted_keys;
        std::vector<UInt64> arrival;
    };
    const std::vector<Case> cases{{1, {2, 1, 3}, {1, 2, 3}, {1, 0, 2}}, {2, {2, 1, 2, 3, 1}, {1, 1, 2, 2, 3}, {1, 4, 0, 2, 3}}};
    for (const auto & [group_limit, input_keys, sorted_keys, arrival] : cases)
    {
        SCOPED_TRACE(group_limit);
        Block block{numbers("k", input_keys), arrivals(input_keys.size())};
        sortBlockAndLimitGroupRows(block, ascending({"k"}), group_limit, IColumn::PermutationSortStability::Stable);

        EXPECT_EQ(values(block, "k"), sorted_keys);
        EXPECT_EQ(values(block, "arrival"), arrival);
    }
}

TEST(SortBlockAndLimitGroupRows, SortedRowsWithinTheLimitStayAsTheyAre)
{
    /// The permutation is the identity and drops no row, so the columns are not copied.
    for (const auto & [group_limit, keys] : std::vector<std::pair<size_t, std::vector<UInt64>>>{{1, {1, 2, 3}}, {2, {1, 1, 2, 3, 3}}})
    {
        SCOPED_TRACE(group_limit);
        Block block{numbers("k", keys), arrivals(keys.size())};
        const auto * column_before = block.getByName("k").column.get();
        sortBlockAndLimitGroupRows(block, ascending({"k"}), group_limit, IColumn::PermutationSortStability::Stable);

        EXPECT_EQ(block.getByName("k").column.get(), column_before);
        std::vector<UInt64> arrival(keys.size());
        std::iota(arrival.begin(), arrival.end(), 0);
        EXPECT_EQ(values(block, "arrival"), arrival);
    }
}

TEST(SortBlockAndLimitGroupRows, DescendingDirection)
{
    struct Case
    {
        size_t group_limit;
        std::vector<UInt64> keys;
        std::vector<UInt64> arrival;
    };
    SortDescription description;
    description.emplace_back("k", -1, 1);
    const std::vector<Case> cases{{1, {3, 1}, {1, 0}}, {2, {3, 3, 1, 1}, {1, 3, 0, 2}}};
    for (const auto & [group_limit, keys, arrival] : cases)
    {
        SCOPED_TRACE(group_limit);
        Block block{numbers("k", {1, 3, 1, 3, 3}), arrivals(5)};
        sortBlockAndLimitGroupRows(block, description, group_limit, IColumn::PermutationSortStability::Stable);

        EXPECT_EQ(values(block, "k"), keys);
        EXPECT_EQ(values(block, "arrival"), arrival);
    }
}

TEST(SortBlockAndLimitGroupRows, NullsAreOneKey)
{
    /// `NULL` values sort last (`nulls_direction` = 1) and form one key, which keeps its first rows.
    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    for (const auto & [group_limit, arrival] : std::vector<std::pair<size_t, std::vector<UInt64>>>{{1, {1, 0}}, {2, {1, 3, 0, 2}}})
    {
        SCOPED_TRACE(group_limit);
        auto nested = ColumnUInt64::create();
        auto null_map = ColumnUInt8::create();
        for (const auto [value, is_null] : std::vector<std::pair<UInt64, UInt8>>{{0, 1}, {1, 0}, {0, 1}, {1, 0}, {0, 1}})
        {
            nested->insertValue(value);
            null_map->insertValue(is_null);
        }
        Block block{{ColumnNullable::create(std::move(nested), std::move(null_map)), nullable_type, "k"}, arrivals(5)};
        sortBlockAndLimitGroupRows(block, ascending({"k"}), group_limit, IColumn::PermutationSortStability::Stable);

        EXPECT_EQ(values(block, "arrival"), arrival);
    }
}

TEST(SortBlockAndLimitGroupRows, EmptyBlock)
{
    for (const size_t group_limit : {1, 2})
    {
        SCOPED_TRACE(group_limit);
        Block block{numbers("k", {}), arrivals(0)};
        sortBlockAndLimitGroupRows(block, ascending({"k"}), group_limit, IColumn::PermutationSortStability::Stable);
        EXPECT_EQ(block.rows(), 0u);
    }
}

TEST(SortBlockAndLimitGroupRows, EqualValuesKeepTheFirstRows)
{
    /// A non-constant column of equal values is a single equal range whose permutation is the identity. Its
    /// rows past the limit are dropped all the same, so the block does not take the path that leaves it
    /// unchanged.
    for (const auto & [group_limit, arrival] : std::vector<std::pair<size_t, std::vector<UInt64>>>{{1, {0}}, {2, {0, 1}}})
    {
        SCOPED_TRACE(group_limit);
        Block block{numbers("k", {7, 7, 7, 7}), arrivals(4)};
        sortBlockAndLimitGroupRows(block, ascending({"k"}), group_limit, IColumn::PermutationSortStability::Stable);

        EXPECT_EQ(values(block, "k"), std::vector<UInt64>(group_limit, 7));
        EXPECT_EQ(values(block, "arrival"), arrival);
    }
}

TEST(SortBlockAndLimitGroupRows, CollationEquivalentStringsKeepTheFirstRows)
{
    /// Collation-equivalent strings form one key, whose retained rows keep their own spelling. Long equal
    /// ranges exercise collation comparisons beyond the initial linear probe.
    struct Case
    {
        size_t group_limit;
        std::vector<String> keys;
        std::vector<UInt64> arrival;
    };
    std::vector<String> input_keys;
    for (size_t i = 0; i < 10; ++i)
        input_keys.insert(input_keys.end(), {"b", "A", "a", "B"});
    SortDescription description;
    description.emplace_back("k", 1, 1, std::make_shared<Collator>("en-u-ks-level2"));
    const std::vector<Case> cases{{1, {"A", "b"}, {1, 0}}, {2, {"A", "a", "b", "B"}, {1, 2, 0, 3}}};
    for (const auto & [group_limit, keys, arrival] : cases)
    {
        SCOPED_TRACE(group_limit);
        Block block{strings("k", input_keys), arrivals(input_keys.size())};
        sortBlockAndLimitGroupRows(block, description, group_limit, IColumn::PermutationSortStability::Stable);

        EXPECT_EQ(stringValues(block, "k"), keys);
        EXPECT_EQ(values(block, "arrival"), arrival);
    }
}

TEST(SortBlockAndLimitGroupRows, MultipleKeysUseCollationForEqualRanges)
{
    struct Case
    {
        size_t group_limit;
        std::vector<UInt64> n;
        std::vector<String> s;
        std::vector<UInt64> arrival;
    };
    SortDescription description;
    description.emplace_back("n", 1, 1);
    description.emplace_back("s", 1, 1, std::make_shared<Collator>("en-u-ks-level2"));
    const std::vector<Case> cases{
        {1, {1, 1, 2, 2}, {"a", "b", "a", "b"}, {4, 1, 0, 5}},
        {2, {1, 1, 1, 2, 2, 2}, {"a", "b", "B", "a", "A", "b"}, {4, 1, 2, 0, 3, 5}}};
    for (const auto & [group_limit, n, s, arrival] : cases)
    {
        SCOPED_TRACE(group_limit);
        Block block{numbers("n", {2, 1, 1, 2, 1, 2, 1}), strings("s", {"a", "b", "B", "A", "a", "b", "b"}), arrivals(7)};
        sortBlockAndLimitGroupRows(block, description, group_limit, IColumn::PermutationSortStability::Stable);

        EXPECT_EQ(values(block, "n"), n);
        EXPECT_EQ(stringValues(block, "s"), s);
        EXPECT_EQ(values(block, "arrival"), arrival);
    }
}

TEST(SortBlockAndLimitGroupRows, GroupLimitPreservesSharedIndexes)
{
    /// The indexes select "two" three times, so a limit of two drops one of its rows when the block is sorted
    /// by the replicated column, and a constant key cuts the block to the limit. The results keep sharing the
    /// indexes.
    const std::vector<UInt64> indexes{2, 0, 2, 1, 0, 2};
    const std::vector<String> left{"two", "zero", "two", "one", "zero", "two"};
    const std::vector<String> right{"2", "0", "2", "1", "0", "2"};
    struct Case
    {
        size_t group_limit;
        std::vector<UInt64> sorted_arrival;
        size_t constant_rows;
    };
    const std::vector<Case> cases{
        {1, {3, 0, 1}, 1}, {2, {3, 0, 2, 1, 4}, 2}, {std::numeric_limits<size_t>::max(), {3, 0, 2, 5, 1, 4}, 6}};
    auto expect_rows = [&](const Block & block, const std::vector<UInt64> & arrival)
    {
        ASSERT_EQ(block.rows(), arrival.size());
        for (size_t row = 0; row < arrival.size(); ++row)
        {
            EXPECT_EQ(block.getByName("arrival").column->getUInt(row), arrival[row]);
            EXPECT_EQ(block.getByName("left").column->getDataAt(row), left[arrival[row]]);
            EXPECT_EQ(block.getByName("right").column->getDataAt(row), right[arrival[row]]);
        }
        checkSharedIndexes(block);
    };
    for (const auto & [group_limit, sorted_arrival, constant_rows] : cases)
    {
        SCOPED_TRACE(group_limit);
        Block block = blockWithSharedIndexes(indexes);
        sortBlockAndLimitGroupRows(block, ascending({"left"}), group_limit, IColumn::PermutationSortStability::Stable);
        expect_rows(block, sorted_arrival);

        Block constant = blockWithSharedIndexes(indexes);
        sortBlockAndLimitGroupRows(constant, ascending({"k"}), group_limit, IColumn::PermutationSortStability::Stable);
        std::vector<UInt64> constant_arrival(constant_rows);
        std::iota(constant_arrival.begin(), constant_arrival.end(), 0);
        expect_rows(constant, constant_arrival);
    }
}
