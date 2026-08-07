#include <gtest/gtest.h>

#include <DataTypes/IDataType.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Common/Exception.h>

using namespace DB::Iceberg;

TEST(IcebergSchemaProcessor, GetSimpleTypeBoolean)
{
    auto type = IcebergSchemaProcessor::getSimpleType("boolean");
    EXPECT_EQ(type->getName(), "Bool");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeInt)
{
    auto type = IcebergSchemaProcessor::getSimpleType("int");
    EXPECT_EQ(type->getName(), "Int32");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeLong)
{
    auto type = IcebergSchemaProcessor::getSimpleType("long");
    EXPECT_EQ(type->getName(), "Int64");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeBigint)
{
    auto type = IcebergSchemaProcessor::getSimpleType("bigint");
    EXPECT_EQ(type->getName(), "Int64");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeFloat)
{
    auto type = IcebergSchemaProcessor::getSimpleType("float");
    EXPECT_EQ(type->getName(), "Float32");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeDouble)
{
    auto type = IcebergSchemaProcessor::getSimpleType("double");
    EXPECT_EQ(type->getName(), "Float64");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeDate)
{
    auto type = IcebergSchemaProcessor::getSimpleType("date");
    EXPECT_EQ(type->getName(), "Date32");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeTime)
{
    auto type = IcebergSchemaProcessor::getSimpleType("time");
    EXPECT_EQ(type->getName(), "Int64");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeTimestamp)
{
    auto type = IcebergSchemaProcessor::getSimpleType("timestamp");
    EXPECT_EQ(type->getName(), "DateTime64(6)");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeTimestamptz)
{
    auto type = IcebergSchemaProcessor::getSimpleType("timestamptz");
    EXPECT_EQ(type->getName(), "DateTime64(6, 'UTC')");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeTimestampNs)
{
    auto type = IcebergSchemaProcessor::getSimpleType("timestamp_ns");
    EXPECT_EQ(type->getName(), "DateTime64(9)");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeTimestamptzNs)
{
    auto type = IcebergSchemaProcessor::getSimpleType("timestamptz_ns");
    EXPECT_EQ(type->getName(), "DateTime64(9, 'UTC')");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeString)
{
    auto type = IcebergSchemaProcessor::getSimpleType("string");
    EXPECT_EQ(type->getName(), "String");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeBinary)
{
    auto type = IcebergSchemaProcessor::getSimpleType("binary");
    EXPECT_EQ(type->getName(), "String");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeUuid)
{
    auto type = IcebergSchemaProcessor::getSimpleType("uuid");
    EXPECT_EQ(type->getName(), "UUID");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeFixed)
{
    auto type = IcebergSchemaProcessor::getSimpleType("fixed[16]");
    EXPECT_EQ(type->getName(), "FixedString(16)");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeDecimal)
{
    auto type = IcebergSchemaProcessor::getSimpleType("decimal(10, 2)");
    EXPECT_EQ(type->getName(), "Decimal(10, 2)");
}

TEST(IcebergSchemaProcessor, GetSimpleTypeUnknownThrows)
{
    EXPECT_THROW(IcebergSchemaProcessor::getSimpleType("unknown_type"), DB::Exception);
}
