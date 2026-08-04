#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
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

GTEST_TEST(NestedUtils, subcolumnOfNestedMember)
{
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();
    DataTypePtr array_type = std::make_shared<DataTypeArray>(uint_type);
    DataTypePtr tuple_type = std::make_shared<DataTypeTuple>(DataTypes{uint_type}, Names{"a"});
    DataTypePtr array_of_tuple_type = std::make_shared<DataTypeArray>(tuple_type);

    /// A member of the Nested group requested only as a subcolumn: the group must still learn about
    /// the member, so the subcolumn shares a name in storage - hence a substreams cache bucket -
    /// with its siblings, which read the same offsets stream.
    const NamesAndTypesList source_columns =
    {
        {"b.arr1", array_type},
        {"b.arr2", "a", array_of_tuple_type, array_type},
    };

    auto nested_type = createNested({uint_type, tuple_type}, {"arr1", "arr2"});
    const NamesAndTypesList expected_subcolumns =
    {
        {"b", "arr1", nested_type, array_type},
        {"b", "arr2.a", nested_type, array_type},
    };

    const NamesAndTypesList expected_nested =
    {
        {"b", nested_type},
    };

    ASSERT_EQ(Nested::convertToSubcolumns(source_columns).toString(), expected_subcolumns.toString());
    ASSERT_EQ(Nested::collect(source_columns).toString(), expected_nested.toString());
}
