#include <Interpreters/Set.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <type_traits>

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
