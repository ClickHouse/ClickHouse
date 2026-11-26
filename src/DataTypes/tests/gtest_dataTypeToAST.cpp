#include <gtest/gtest.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/dataTypeToAST.h>
#include <Parsers/ASTDataType.h>

using namespace DB;

static void check(const std::string & type_name)
{
    auto type = DataTypeFactory::instance().get(type_name);
    auto ast = dataTypeToAST(type);
    auto new_type = DataTypeFactory::instance().get(ast);
    EXPECT_EQ(type->getName(), new_type->getName());
}

TEST(DataTypeToAST, SimpleTypes)
{
    check("String");
    check("UInt64");
    check("Float64");
    check("Date");
    check("DateTime");
}

TEST(DataTypeToAST, ParameterizedTypes)
{
    check("FixedString(16)");
    check("Decimal(9, 2)");
    check("DateTime('UTC')");
    check("DateTime64(3)");
    check("DateTime64(3, 'UTC')");
}

TEST(DataTypeToAST, CompositeTypes)
{
    check("Nullable(DateTime64(3))");
    check("Array(Array(UInt8))");
    check("LowCardinality(Nullable(String))");
    check("Enum8('a' = 1, 'b' = 2)");
    check("Enum16('x' = 1, 'y' = 1000, 'z' = -1000)");
    check("Tuple(String, Nullable(UInt64), Array(String))");
    check("Tuple(key String, value Map(String, UInt64))");
    check("Map(String, Map(String, UInt8))");
    check("Variant(String, UInt64)");
}

TEST(DataTypeToAST, NestedDataType)
{
    check("Nested(a Int8, b String)");
    check("Nested(id UInt64, value String, ts DateTime64(3))");
}
