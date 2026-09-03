#include <Interpreters/validateParameterizedViewSchema.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ColumnsDescription.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(ValidateParameterizedViewSchema, EmptyDeclaredColumnsDoesNotThrow)
{
    NamesAndTypesList actual = {{"n", std::make_shared<DataTypeUInt64>()}};
    ColumnsDescription declared;
    EXPECT_NO_THROW(validateParameterizedViewSchema("test_view", actual, declared));
}

TEST(ValidateParameterizedViewSchema, MatchingSchemaDoesNotThrow)
{
    NamesAndTypesList actual = {
        {"n", std::make_shared<DataTypeUInt64>()},
        {"s", std::make_shared<DataTypeString>()},
    };
    ColumnsDescription declared;
    declared.add(ColumnDescription("n", std::make_shared<DataTypeUInt64>()));
    declared.add(ColumnDescription("s", std::make_shared<DataTypeString>()));
    EXPECT_NO_THROW(validateParameterizedViewSchema("test_view", actual, declared));
}

TEST(ValidateParameterizedViewSchema, ColumnCountMismatchThrows)
{
    NamesAndTypesList actual = {{"n", std::make_shared<DataTypeUInt64>()}};
    ColumnsDescription declared;
    declared.add(ColumnDescription("n", std::make_shared<DataTypeUInt64>()));
    declared.add(ColumnDescription("s", std::make_shared<DataTypeString>()));
    EXPECT_THROW(validateParameterizedViewSchema("test_view", actual, declared), Exception);
}

TEST(ValidateParameterizedViewSchema, ColumnNameMismatchThrows)
{
    NamesAndTypesList actual = {{"x", std::make_shared<DataTypeUInt64>()}};
    ColumnsDescription declared;
    declared.add(ColumnDescription("n", std::make_shared<DataTypeUInt64>()));
    EXPECT_THROW(validateParameterizedViewSchema("test_view", actual, declared), Exception);
}

TEST(ValidateParameterizedViewSchema, ColumnTypeMismatchThrows)
{
    NamesAndTypesList actual = {{"n", std::make_shared<DataTypeString>()}};
    ColumnsDescription declared;
    declared.add(ColumnDescription("n", std::make_shared<DataTypeUInt64>()));
    EXPECT_THROW(validateParameterizedViewSchema("test_view", actual, declared), Exception);
}

TEST(ValidateParameterizedViewSchema, SingleColumnMatchDoesNotThrow)
{
    NamesAndTypesList actual = {{"n", std::make_shared<DataTypeUInt64>()}};
    ColumnsDescription declared;
    declared.add(ColumnDescription("n", std::make_shared<DataTypeUInt64>()));
    EXPECT_NO_THROW(validateParameterizedViewSchema("test_view", actual, declared));
}
