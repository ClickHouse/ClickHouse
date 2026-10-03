#include <config.h>

#include <gtest/gtest.h>

#if USE_AVRO

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

using namespace DB;

namespace
{

std::string icebergTypeName(DataTypePtr type)
{
    Int32 iter = 0;
    auto [iceberg_type, required] = Iceberg::getIcebergType(type, iter);
    return iceberg_type.convert<std::string>();
}

}

TEST(IcebergTypeMapping, DateTimeWritesAsTimestamptz)
{
    EXPECT_EQ(icebergTypeName(std::make_shared<DataTypeDateTime>()), Iceberg::f_timestamptz);
}

TEST(IcebergTypeMapping, DateTime64MicrosWritesAsTimestamptz)
{
    EXPECT_EQ(icebergTypeName(std::make_shared<DataTypeDateTime64>(6)), Iceberg::f_timestamptz);
    EXPECT_EQ(icebergTypeName(std::make_shared<DataTypeDateTime64>(6, "UTC")), Iceberg::f_timestamptz);
}

TEST(IcebergTypeMapping, DateTime64NanosWritesAsTimestamptzNs)
{
    EXPECT_EQ(icebergTypeName(std::make_shared<DataTypeDateTime64>(9)), Iceberg::f_timestamptz_ns);
    EXPECT_EQ(icebergTypeName(std::make_shared<DataTypeDateTime64>(9, "UTC")), Iceberg::f_timestamptz_ns);
}

#endif
