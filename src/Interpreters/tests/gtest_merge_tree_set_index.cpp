#include <Interpreters/Set.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

using namespace DB;

TEST(MergeTreeSetIndex, checkInRangeOne)
{
    DataTypes types = {std::make_shared<const DataTypeInt64>()};

    auto mut = types[0]->createColumn();
    mut->insert(1);
    mut->insert(5);
    mut->insert(7);

    Columns columns = {std::move(mut)};

    std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> mapping = {{0, 0, {}}};
    auto set = std::make_unique<MergeTreeSetIndex>(columns, std::move(mapping));

    // Left and right bounded
    Ranges ranges = {Range(1, true, 4, true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "(1, 4)";

    ranges = {Range(2, true, 4, true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, false) << "(2, 4)";

    ranges = {Range(-1, true, 0, true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, false) << "(-1, 0)";

    ranges = {Range(-1, true, 10, true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "(-1, 10)";

    // Left bounded
    ranges = {Range::createLeftBounded(1, true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "(1, +inf)";

    ranges = {Range::createLeftBounded(-1, true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "(-1, +inf)";

    ranges = {Range::createLeftBounded(10, true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, false) << "(10, +inf)";

    // Right bounded
    ranges = {Range::createRightBounded(1, true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "(-inf, 1)";

    ranges = {Range::createRightBounded(-1, true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, false) << "(-inf, -1)";

    ranges = {Range::createRightBounded(10, true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "(-inf, 10)";
}

TEST(MergeTreeSetIndex, checkInRangeTuple)
{
    DataTypes types = {std::make_shared<const DataTypeUInt64>(), std::make_shared<const DataTypeString>()};

    Columns columns;
    {
        auto values = {1, 1, 3, 3, 3, 10};
        auto mut = types[0]->createColumn();
        for (const auto & val : values)
            mut->insert(val);
        columns.push_back(std::move(mut));
    }

    {
        auto values = {"a", "b", "a", "a", "b", "c"};
        auto mut = types[1]->createColumn();
        for (const auto & val : values)
            mut->insert(val);
        columns.push_back(std::move(mut));
    }

    std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> mapping = {{0, 0, {}}, {1, 1, {}}};
    auto set = std::make_unique<MergeTreeSetIndex>(columns, std::move(mapping));

    Ranges ranges = {Range(1), Range("a", true, "c", true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "Range(1), Range('a', true, 'c', true)";

    ranges = {Range(1, false, 3, false), Range::createWholeUniverseWithoutNull()};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, false) << "Range(1, false, 3, false), Range::createWholeUniverseWithoutNull()";

    ranges = {Range(2, false, 5, false), Range::createWholeUniverseWithoutNull()};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "Range(2, false, 5, false), Range::createWholeUniverseWithoutNull()";

    ranges = {Range(3), Range::createLeftBounded("a", true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "Range(3), Range::createLeftBounded('a', true)";

    ranges = {Range(3), Range::createLeftBounded("f", true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, false) << "Range(3), Range::createLeftBounded('f', true)";

    ranges = {Range(3), Range::createRightBounded("a", true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "Range(3), Range::createRightBounded('a', true)";

    ranges = {Range(3), Range::createRightBounded("b", true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "Range(3), Range::createRightBounded('b', true)";

    ranges = {Range(1), Range("b")};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "Range(1), Range('b')";

    ranges = {Range(1), Range("c")};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, false) << "Range(1), Range('c')";

    ranges = {Range(2, true, 3, true), Range::createWholeUniverseWithoutNull()};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, true) << "Range(2, true, 3, true), Range('x', true, 'z', true)";

    ranges = {Range(2), Range("a", true, "z", true)};
    ASSERT_EQ(set->checkInRange(ranges, types, 0).can_be_true, false) << "Range(2, true, 3, true), Range('c', true, 'z', true)";
}

namespace
{

template <typename T>
void checkSingleIntegerColumn()
{
    DataTypes types = {std::make_shared<DataTypeNumber<T>>()};
    for (const std::vector<Int64> & values : {std::vector<Int64>{}, std::vector<Int64>{1}, std::vector<Int64>{1, 5, 5, 7}})
    {
        auto column = types[0]->createColumn();
        for (Int64 value : values)
            column->insert(Field(value));
        MergeTreeSetIndex index({std::move(column)}, {{0, 0, {}}});
        for (Int64 left = 0; left <= 9; ++left)
        {
            for (Int64 right = left; right <= 9; ++right)
            {
                for (unsigned flags = 0; flags < 4; ++flags)
                {
                    SCOPED_TRACE(::testing::Message() << "width=" << sizeof(T) << " signed=" << std::is_signed_v<T>
                        << " size=" << values.size() << " left=" << left << " right=" << right << " flags=" << flags);
                    bool left_included = flags & 1;
                    bool right_included = flags & 2;
                    bool expected = std::any_of(values.begin(), values.end(), [&](Int64 value)
                    {
                        return (value > left || (value == left && left_included))
                            && (value < right || (value == right && right_included));
                    });
                    /// Set open bounds directly so Range does not normalize them to different integer endpoints.
                    Ranges ranges = {Range(Field(left))};
                    ranges[0].right = Field(right);
                    ranges[0].left_included = left_included;
                    ranges[0].right_included = right_included;
                    auto check = [&](BoolMask mask)
                    {
                        EXPECT_EQ(mask.can_be_true, expected);
                        EXPECT_EQ(mask.can_be_false, left == right ? !expected : true);
                    };
                    check(index.checkInRange(ranges, types, 0));
                    check(index.checkInRange({0}, ranges, types, 0));
                }
            }
        }
    }

    auto field = [](T value) { return Field(static_cast<NearestFieldType<T>>(value)); };
    const std::vector<T> limits = {std::numeric_limits<T>::lowest(), T(0), std::numeric_limits<T>::max()};
    auto column = types[0]->createColumn();
    for (T value : limits)
        column->insert(field(value));
    MergeTreeSetIndex index({std::move(column)}, {{0, 0, {}}});
    for (T left : limits)
    {
        for (T right : limits)
        {
            if (left > right)
                continue;
            for (unsigned flags = 0; flags < 4; ++flags)
            {
                bool expected = std::any_of(limits.begin(), limits.end(), [&](T value)
                {
                    return (value > left || (value == left && (flags & 1)))
                        && (value < right || (value == right && (flags & 2)));
                });
                Ranges ranges = {Range(field(left))};
                ranges[0].right = field(right);
                ranges[0].left_included = flags & 1;
                ranges[0].right_included = flags & 2;
                for (BoolMask mask : {index.checkInRange(ranges, types, 0), index.checkInRange({0}, ranges, types, 0)})
                {
                    EXPECT_EQ(mask.can_be_true, expected);
                    EXPECT_EQ(mask.can_be_false, left == right ? !expected : true);
                }
            }
        }
    }
}

/// An end of a range of `Int64` tuples, compared lexicographically with the rows of the set. The second element of a
/// pair is 0 for a row value and for an included bound; an excluded left end lies just above its value (+1), an
/// excluded right end just below it (-1). `nullopt` is an infinity.
using LexPoint = std::optional<std::vector<std::pair<Int64, int>>>;

/// The rows of the generic path tests, with duplicates and a gap at 2 in the first element.
const std::vector<std::vector<Int64>> & genericPathRows()
{
    static const std::vector<std::vector<Int64>> rows = {{0, 1}, {0, 3}, {1, 0}, {1, 0}, {1, 2}, {1, 2}, {3, 0}, {3, 1}, {3, 3}};
    return rows;
}

bool anyRowInLexRange(const LexPoint & left, const LexPoint & right)
{
    return std::ranges::any_of(genericPathRows(), [&](const std::vector<Int64> & row)
    {
        std::vector<std::pair<Int64, int>> point;
        for (Int64 value : row)
            point.emplace_back(value, 0);
        return (!left || *left <= point) && (!right || point <= *right);
    });
}

void checkGenericPath(
    const MergeTreeSetIndex & index, const DataTypes & types, const Ranges & ranges,
    const LexPoint & left, const LexPoint & right, bool all_bounds_equal)
{
    bool expected = anyRowInLexRange(left, right);
    std::vector<int> key_col_to_sparse_pos;
    for (size_t i = 0; i < ranges.size(); ++i)
        key_col_to_sparse_pos.push_back(static_cast<int>(i));
    for (BoolMask mask : {index.checkInRange(ranges, types, 0), index.checkInRange(key_col_to_sparse_pos, ranges, types, 0)})
    {
        EXPECT_EQ(mask.can_be_true, expected);
        EXPECT_EQ(mask.can_be_false, all_bounds_equal ? !expected : true);
    }
}

/// Builds the range directly, so that `Range` does not normalize open integer bounds.
Range makeRange(const Field & left, bool left_included, const Field & right, bool right_included)
{
    Range range(left);
    range.right = right;
    range.left_included = left_included;
    range.right_included = right_included;
    return range;
}

struct Bound
{
    Int64 left;
    Int64 right;
    bool left_included;
    bool right_included;
};

/// Every range of one `Int64` key column with ends in [-1, 4], including the empty ones.
std::vector<Bound> genericPathBounds()
{
    std::vector<Bound> bounds;
    for (Int64 left = -1; left <= 4; ++left)
        for (Int64 right = left; right <= 4; ++right)
            for (unsigned flags = 0; flags < 4; ++flags)
                bounds.push_back({left, right, (flags & 1) != 0, (flags & 2) != 0});
    return bounds;
}

}

TEST(MergeTreeSetIndex, SingleIntegerColumnBounds)
{
    checkSingleIntegerColumn<UInt8>();
    checkSingleIntegerColumn<UInt16>();
    checkSingleIntegerColumn<UInt32>();
    checkSingleIntegerColumn<UInt64>();
    checkSingleIntegerColumn<Int8>();
    checkSingleIntegerColumn<Int16>();
    checkSingleIntegerColumn<Int32>();
    checkSingleIntegerColumn<Int64>();
}

TEST(MergeTreeSetIndex, GenericPathBoundsKeyColumns)
{
    auto int64 = std::make_shared<DataTypeInt64>();
    DataTypes types = {int64, int64};
    Columns columns;
    for (size_t i = 0; i < 2; ++i)
    {
        auto column = int64->createColumn();
        for (const auto & row : genericPathRows())
            column->insert(Field(row[i]));
        columns.push_back(std::move(column));
    }
    MergeTreeSetIndex index(columns, {{0, 0, {}}, {1, 1, {}}});

    const std::vector<Bound> bounds = genericPathBounds();
    for (const Bound & first : bounds)
    {
        for (const Bound & second : bounds)
        {
            SCOPED_TRACE(::testing::Message() << "left=(" << first.left << ", " << second.left << ") right=("
                << first.right << ", " << second.right << ") left_included=(" << first.left_included << ", "
                << second.left_included << ") right_included=(" << first.right_included << ", " << second.right_included << ")");
            Ranges ranges = {
                makeRange(Field(first.left), first.left_included, Field(first.right), first.right_included),
                makeRange(Field(second.left), second.left_included, Field(second.right), second.right_included)};
            LexPoint left = std::vector<std::pair<Int64, int>>{
                {first.left, first.left_included ? 0 : 1}, {second.left, second.left_included ? 0 : 1}};
            LexPoint right = std::vector<std::pair<Int64, int>>{
                {first.right, first.right_included ? 0 : -1}, {second.right, second.right_included ? 0 : -1}};
            checkGenericPath(index, types, ranges, left, right, first.left == first.right && second.left == second.right);
        }
    }
}

/// All ranges are checked by one index, so that its range buffers are reused, also between a bound and an infinity.
TEST(MergeTreeSetIndex, GenericPathBoundsTupleKey)
{
    auto int64 = std::make_shared<DataTypeInt64>();
    DataTypePtr tuple_type = std::make_shared<DataTypeTuple>(DataTypes{int64, int64});
    DataTypePtr nullable_tuple_type = std::make_shared<DataTypeNullable>(tuple_type);

    std::vector<std::pair<Int64, Int64>> points;
    for (Int64 first = -1; first <= 4; ++first)
        for (Int64 second = -1; second <= 4; ++second)
            points.emplace_back(first, second);
    auto tuple = [](std::pair<Int64, Int64> point) { return Field(Tuple{Field(point.first), Field(point.second)}); };
    auto lex = [](std::pair<Int64, Int64> point, int epsilon)
    {
        return LexPoint(std::vector<std::pair<Int64, int>>{{point.first, 0}, {point.second, epsilon}});
    };

    for (const DataTypePtr & type : {tuple_type, nullable_tuple_type})
    {
        SCOPED_TRACE(type->getName());
        DataTypes types = {type};
        auto column = type->createColumn();
        for (const auto & row : genericPathRows())
            column->insert(Field(Tuple{Field(row[0]), Field(row[1])}));
        MergeTreeSetIndex index({std::move(column)}, {{0, 0, {}}});

        for (size_t i = 0; i < points.size(); ++i)
        {
            for (bool included : {false, true})
            {
                SCOPED_TRACE(::testing::Message() << "point=(" << points[i].first << ", " << points[i].second
                    << ") included=" << included);
                checkGenericPath(index, types, {Range::createLeftBounded(tuple(points[i]), included)},
                    lex(points[i], included ? 0 : 1), std::nullopt, false);
                checkGenericPath(index, types, {Range::createRightBounded(tuple(points[i]), included)},
                    std::nullopt, lex(points[i], included ? 0 : -1), false);
            }
            for (size_t j = i; j < points.size(); ++j)
            {
                for (unsigned flags = 0; flags < 4; ++flags)
                {
                    bool left_included = flags & 1;
                    bool right_included = flags & 2;
                    SCOPED_TRACE(::testing::Message() << "left=(" << points[i].first << ", " << points[i].second
                        << ") right=(" << points[j].first << ", " << points[j].second << ") left_included=" << left_included
                        << " right_included=" << right_included);
                    checkGenericPath(index, types, {makeRange(tuple(points[i]), left_included, tuple(points[j]), right_included)},
                        lex(points[i], left_included ? 0 : 1), lex(points[j], right_included ? 0 : -1), i == j);
                }
            }
        }
    }
}
