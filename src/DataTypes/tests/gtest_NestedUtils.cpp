#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNested.h>
#include <gtest/gtest.h>

using namespace DB;

GTEST_TEST(NestedUtils, collect)
{
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();
    DataTypePtr array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());

    const NamesAndTypesList source_columns =
    {
        {"id", uint_type},
        {"arr1", array_type},
        {"b.id", uint_type},
        {"b.arr1", array_type},
        {"b.arr2", array_type}
    };

    auto nested_type = createNested({uint_type, uint_type}, {"arr1", "arr2"});
    const NamesAndTypesList columns_with_subcolumns =
    {
        {"id", uint_type},
        {"arr1", array_type},
        {"b.id", uint_type},
        {"b", "arr1", nested_type, array_type},
        {"b", "arr2", nested_type, array_type}
    };

    const NamesAndTypesList columns_with_nested =
    {
        {"id", uint_type},
        {"arr1", array_type},
        {"b.id", uint_type},
        {"b", nested_type},
    };

    ASSERT_EQ(Nested::convertToSubcolumns(source_columns).toString(), columns_with_subcolumns.toString());
    ASSERT_EQ(Nested::collect(source_columns).toString(), columns_with_nested.toString());
}
