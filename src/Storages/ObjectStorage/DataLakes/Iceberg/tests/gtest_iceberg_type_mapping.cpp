#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

using namespace DB;
using namespace DB::Iceberg;

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

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

/// The Iceberg spec caps `decimal(P, S)` at precision 38, while ClickHouse `Decimal256`
/// goes up to 76. A wider precision has no representation in Iceberg, so it must be
/// refused rather than serialized into metadata other engines would reject.
namespace
{

constexpr UInt32 iceberg_max_decimal_precision = 38;

void expectIcebergTypeRejected(const DataTypePtr & type)
{
    Int32 iter = 0;
    try
    {
        auto [iceberg_type, required] = getIcebergType(type, iter);
        FAIL() << type->getName() << " has no Iceberg representation but was mapped to "
               << iceberg_type.toString();
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS) << type->getName() << ": " << e.message();
    }
}

}

TEST(IcebergTypeMapping, EveryDecimalPrecisionWithinSpecLimitMaps)
{
    for (UInt32 precision = 1; precision <= iceberg_max_decimal_precision; ++precision)
    {
        auto type = createDecimal<DataTypeDecimal>(precision, 1);
        Int32 iter = 0;
        auto [iceberg_type, required] = getIcebergType(type, iter);
        ASSERT_TRUE(iceberg_type.isString()) << "precision " << precision;
        EXPECT_EQ(iceberg_type.extract<String>(), "decimal(" + std::to_string(precision) + ", 1)");
        EXPECT_TRUE(required);
    }
}

TEST(IcebergTypeMapping, DecimalPrecisionAboveSpecLimitIsRejected)
{
    for (UInt32 precision : {iceberg_max_decimal_precision + 1, 50u, 76u})
        expectIcebergTypeRejected(createDecimal<DataTypeDecimal>(precision, 1));
}

TEST(IcebergTypeMapping, NullableDecimalPrecisionAboveSpecLimitIsRejected)
{
    expectIcebergTypeRejected(makeNullable(createDecimal<DataTypeDecimal>(76, 1)));
}

#endif
