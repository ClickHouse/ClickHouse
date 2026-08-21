#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <gtest/gtest.h>
#include "boost/geometry/strategies/concepts/within_concept.hpp"

using namespace DB;

MutableColumnPtr createNestedColumn(const std::vector<String> & values)
{
    MutableColumnPtr nested_column = ColumnString::create();
    for (const auto & value : values)
        nested_column->insert(value);
    return nested_column;
}

ColumnReplicated::MutablePtr createColumn(const std::vector<String> & values, const std::vector<size_t> & indexes)
{
    MutableColumnPtr nested_column = createNestedColumn(values);
    MutableColumnPtr indexes_column = ColumnUInt8::create();
    for (const auto & index : indexes)
        indexes_column->insert(index);

    return ColumnReplicated::create(std::move(nested_column), std::move(indexes_column));
}

void checkColumn(const ColumnReplicated & column, const std::vector<String> & expected_values, const std::vector<size_t> & expected_indexes)
{
    const auto & nested_column = column.getNestedColumn();
    ASSERT_EQ(nested_column->size(), expected_values.size());
    for (size_t i = 0; i < expected_values.size(); ++i)
        ASSERT_EQ((*nested_column)[i], Field(expected_values[i]));

    const auto & indexes = column.getIndexes().getIndexes();
    ASSERT_EQ(indexes->size(), expected_indexes.size());
    for (size_t i = 0; i < expected_indexes.size(); ++i)
        ASSERT_EQ((*indexes)[i], Field(expected_indexes[i]));
}

void checkColumn(const IColumn & column, const std::vector<String> & expected_values, const std::vector<size_t> & expected_indexes)
{
    checkColumn(assert_cast<const ColumnReplicated &>(column), expected_values, expected_indexes);
}

TEST(ColumnReplicated, CloneResized)
{
    auto column = createColumn({"s1", "s2", "s3"}, {0, 1, 1, 2, 0, 0, 1, 2, 0});
    auto resized_column = column->cloneResized(6);
    checkColumn(assert_cast<const ColumnReplicated &>(*resized_column), {"s1", "s2", "s3"}, {0, 1, 1, 2, 0, 0});
    resized_column = resized_column->cloneResized(3);
    checkColumn(assert_cast<const ColumnReplicated &>(*resized_column), {"s1", "s2"}, {0, 1, 1});
    resized_column = resized_column->cloneResized(1);
    checkColumn(assert_cast<const ColumnReplicated &>(*resized_column), {"s1"}, {0});
    resized_column = resized_column->cloneResized(3);
    checkColumn(assert_cast<const ColumnReplicated &>(*resized_column), {"s1", ""}, {0, 1, 1});
}


TEST(ColumnReplicated, PopBack)
{
    auto column = createColumn({"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0, 1, 2, 0});
    column->popBack(3);
    checkColumn(*column, {"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0});
    column->popBack(3);
    checkColumn(*column, {"s2", "s3"}, {1, 0, 0});
    column->popBack(2);
    checkColumn(*column, {"s3"}, {0});
}

TEST(ColumnReplicated, Filter)
{
    auto column = createColumn({"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0, 1, 2, 0});
    IColumnFilter filter = {0, 0, 0, 1, 1, 0, 0, 0, 0};
    auto filtered_column = column->filter(filter, 2);
    checkColumn(*filtered_column, {"s1", "s3"}, {1, 0});
}

TEST(ColumnReplicated, Index)
{
    auto column = createColumn({"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0, 1, 2, 0});
    auto index_column = ColumnUInt64::create();
    index_column->getData() = {3, 4};
    auto filtered_column = column->index(*index_column, 0);
    checkColumn(*filtered_column, {"s1", "s3"}, {1, 0});
}

TEST(ColumnReplicated, Permute)
{
    auto column = createColumn({"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0, 1, 2, 0});
    IColumnPermutation permutation = {3, 4, 0, 1, 2, 5, 6, 7, 8};
    auto filtered_column = column->permute(permutation, 2);
    checkColumn(*filtered_column, {"s1", "s3"}, {1, 0});
}

TEST(ColumnReplicated, InsertRangeFrom)
{
    auto column_to = createColumn({"s0", "s1"}, {0, 1});
    auto column_from = createColumn({"s2", "s3", "s4"}, {0, 1, 1, 2, 0, 0, 1, 2, 0});
    column_to->insertRangeFrom(*column_from, 3, 4);
    checkColumn(*column_to, {"s0", "s1", "s4", "s2", "s3"}, {0, 1, 2, 3, 3, 4});
}

TEST(ColumnReplicated, IndicesOfNonDefaultRows)
{
    auto column = createColumn({"s1", "s2", "", "s3", ""}, {0, 1, 1, 3, 2, 0, 0, 3, 1, 2, 0, 3, 4});
    IColumn::Offsets offsets;
    column->getIndicesOfNonDefaultRows(offsets, 0, column->size());
    ASSERT_EQ(offsets.size(), 10);
    ASSERT_EQ(offsets, IColumn::Offsets({0, 1, 2, 3, 5, 6, 7, 8, 10, 11}));
}
