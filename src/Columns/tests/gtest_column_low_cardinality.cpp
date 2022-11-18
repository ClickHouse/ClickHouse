#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnString.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(ColumnLowCardinality, InsertRange) {
    auto int_data_type = std::make_shared<DataTypeInt64>();
    auto low_cardinality_data_type = std::make_shared<DataTypeLowCardinality>(std::move(int_data_type));

    auto create_column = [&]() { return low_cardinality_data_type->createColumn(); };

//    assertColumnPermutations(create_column, IndexInRangeInt64Transform());
}
