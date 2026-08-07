#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

using namespace DB;
using namespace DB::Iceberg;

TEST(IcebergTypeMapping, BoolMapsToBoolean)
{
    auto bool_type = DataTypeFactory::instance().get("Bool");
    Int32 iter = 0;
    auto [iceberg_type, required] = getIcebergType(bool_type, iter);
    ASSERT_TRUE(iceberg_type.isString());
    EXPECT_EQ(iceberg_type.extract<String>(), "boolean");
    EXPECT_TRUE(required);
}

TEST(IcebergTypeMapping, NullableBoolMapsToBoolean)
{
    auto bool_type = makeNullable(DataTypeFactory::instance().get("Bool"));
    Int32 iter = 0;
    auto [iceberg_type, required] = getIcebergType(bool_type, iter);
    ASSERT_TRUE(iceberg_type.isString());
    EXPECT_EQ(iceberg_type.extract<String>(), "boolean");
    EXPECT_FALSE(required);
}

TEST(IcebergTypeMapping, UInt8MapsToInt)
{
    auto type = std::make_shared<DataTypeUInt8>();
    Int32 iter = 0;
    auto [iceberg_type, required] = getIcebergType(type, iter);
    ASSERT_TRUE(iceberg_type.isString());
    EXPECT_EQ(iceberg_type.extract<String>(), "int");
    EXPECT_TRUE(required);
}

TEST(IcebergTypeMapping, Int8MapsToInt)
{
    auto type = std::make_shared<DataTypeInt8>();
    Int32 iter = 0;
    auto [iceberg_type, required] = getIcebergType(type, iter);
    ASSERT_TRUE(iceberg_type.isString());
    EXPECT_EQ(iceberg_type.extract<String>(), "int");
}

TEST(IcebergTypeMapping, UInt16MapsToInt)
{
    auto type = std::make_shared<DataTypeUInt16>();
    Int32 iter = 0;
    auto [iceberg_type, required] = getIcebergType(type, iter);
    ASSERT_TRUE(iceberg_type.isString());
    EXPECT_EQ(iceberg_type.extract<String>(), "int");
}

TEST(IcebergTypeMapping, Int16MapsToInt)
{
    auto type = std::make_shared<DataTypeInt16>();
    Int32 iter = 0;
    auto [iceberg_type, required] = getIcebergType(type, iter);
    ASSERT_TRUE(iceberg_type.isString());
    EXPECT_EQ(iceberg_type.extract<String>(), "int");
}

TEST(IcebergTypeMapping, Decimal32MapsToDecimal)
{
    auto type = std::make_shared<DataTypeDecimal<Decimal32>>(9, 2);
    Int32 iter = 0;
    auto [iceberg_type, required] = getIcebergType(type, iter);
    ASSERT_TRUE(iceberg_type.isString());
    EXPECT_EQ(iceberg_type.extract<String>(), "decimal(9, 2)");
    EXPECT_TRUE(required);
}

TEST(IcebergTypeMapping, Decimal64MapsToDecimal)
{
    auto type = std::make_shared<DataTypeDecimal<Decimal64>>(18, 5);
    Int32 iter = 0;
    auto [iceberg_type, required] = getIcebergType(type, iter);
    ASSERT_TRUE(iceberg_type.isString());
    EXPECT_EQ(iceberg_type.extract<String>(), "decimal(18, 5)");
}

TEST(IcebergTypeMapping, Decimal128MapsToDecimal)
{
    auto type = std::make_shared<DataTypeDecimal<Decimal128>>(38, 10);
    Int32 iter = 0;
    auto [iceberg_type, required] = getIcebergType(type, iter);
    ASSERT_TRUE(iceberg_type.isString());
    EXPECT_EQ(iceberg_type.extract<String>(), "decimal(38, 10)");
}

TEST(IcebergTypeMapping, NullableDecimalMapsToDecimalNotRequired)
{
    auto type = makeNullable(std::make_shared<DataTypeDecimal<Decimal32>>(7, 3));
    Int32 iter = 0;
    auto [iceberg_type, required] = getIcebergType(type, iter);
    ASSERT_TRUE(iceberg_type.isString());
    EXPECT_EQ(iceberg_type.extract<String>(), "decimal(7, 3)");
    EXPECT_FALSE(required);
}

#endif
