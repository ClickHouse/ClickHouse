#include <Interpreters/Set.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <gtest/gtest.h>

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
    std::vector<Range> ranges = {Range(1, true, 4, true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "(1, 4)";

    ranges = {Range(2, true, 4, true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, false) << "(2, 4)";

    ranges = {Range(-1, true, 0, true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, false) << "(-1, 0)";

    ranges = {Range(-1, true, 10, true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "(-1, 10)";

    // Left bounded
    ranges = {Range::createLeftBounded(1, true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "(1, +inf)";

    ranges = {Range::createLeftBounded(-1, true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "(-1, +inf)";

    ranges = {Range::createLeftBounded(10, true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, false) << "(10, +inf)";

    // Right bounded
    ranges = {Range::createRightBounded(1, true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "(-inf, 1)";

    ranges = {Range::createRightBounded(-1, true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, false) << "(-inf, -1)";

    ranges = {Range::createRightBounded(10, true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "(-inf, 10)";
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

    std::vector<Range> ranges = {Range(1), Range("a", true, "c", true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "Range(1), Range('a', true, 'c', true)";

    ranges = {Range(1, false, 3, false), Range()};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, false) << "Range(1, false, 3, false), Range()";

    ranges = {Range(2, false, 5, false), Range()};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "Range(2, false, 5, false), Range()";

    ranges = {Range(3), Range::createLeftBounded("a", true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "Range(3), Range::createLeftBounded('a', true)";

    ranges = {Range(3), Range::createLeftBounded("f", true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, false) << "Range(3), Range::createLeftBounded('f', true)";

    ranges = {Range(3), Range::createRightBounded("a", true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "Range(3), Range::createRightBounded('a', true)";

    ranges = {Range(3), Range::createRightBounded("b", true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "Range(3), Range::createRightBounded('b', true)";

    ranges = {Range(1), Range("b")};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "Range(1), Range('b')";

    ranges = {Range(1), Range("c")};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, false) << "Range(1), Range('c')";

    ranges = {Range(2, true, 3, true), Range()};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, true) << "Range(2, true, 3, true), Range('x', true, 'z', true)";

    ranges = {Range(2), Range("a", true, "z", true)};
    ASSERT_EQ(set->checkInRange(ranges, types).can_be_true, false) << "Range(2, true, 3, true), Range('c', true, 'z', true)";
}
