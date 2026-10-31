#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>

#include <base/unaligned.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergFieldParseHelpers.h>

using namespace DB;
using namespace DB::Iceberg;

namespace
{

std::string littleEndianInt64(Int64 value)
{
    std::string bytes(sizeof(Int64), '\0');
    unalignedStoreLittleEndian<Int64>(bytes.data(), value);
    return bytes;
}

Int64 decimal64Ticks(const Field & field)
{
    return static_cast<Int64>(field.safeGet<DecimalField<DateTime64>>().getValue());
}

}

TEST(IcebergFieldParseHelpers, MicrosecondDateTime64BoundsAreKept)
{
    auto type = std::make_shared<DataTypeDateTime64>(6);
    /// 2024-02-20 10:00:00 UTC as microseconds — spec-correct Iceberg `timestamp`.
    constexpr Int64 us_2024_02_20 = 1'708'423'200'000'000LL;
    auto lower = deserializeFieldFromBinaryRepr(littleEndianInt64(us_2024_02_20), type, true);
    auto upper = deserializeFieldFromBinaryRepr(littleEndianInt64(us_2024_02_20), type, false);
    ASSERT_TRUE(lower.has_value());
    ASSERT_TRUE(upper.has_value());
    EXPECT_EQ(decimal64Ticks(*lower), us_2024_02_20);
    EXPECT_EQ(decimal64Ticks(*upper), us_2024_02_20);

    /// 1960-01-01 00:00:00 UTC as microseconds.
    constexpr Int64 us_1960_01_01 = -315'619'200'000'000LL;
    auto negative = deserializeFieldFromBinaryRepr(littleEndianInt64(us_1960_01_01), type, true);
    ASSERT_TRUE(negative.has_value());
    EXPECT_EQ(decimal64Ticks(*negative), us_1960_01_01);

    /// Upper edge of the microsecond band (~year 2286).
    constexpr Int64 one_e16 = 10'000'000'000'000'000LL;
    auto edge = deserializeFieldFromBinaryRepr(littleEndianInt64(one_e16), type, false);
    ASSERT_TRUE(edge.has_value());
    EXPECT_EQ(decimal64Ticks(*edge), one_e16);
}

TEST(IcebergFieldParseHelpers, ProvenNanosecondDateTime64BoundsAreConverted)
{
    auto type = std::make_shared<DataTypeDateTime64>(6);
    constexpr Int64 ns_2024_01_01 = 1'704'067'200'123'456'789LL;
    auto field = deserializeFieldFromBinaryRepr(littleEndianInt64(ns_2024_01_01), type, true);
    ASSERT_TRUE(field.has_value());
    EXPECT_EQ(decimal64Ticks(*field), ns_2024_01_01 / 1000);
}

TEST(IcebergFieldParseHelpers, FarFutureMicrosecondDateTime64BoundsFailOpen)
{
    auto type = std::make_shared<DataTypeDateTime64>(6);
    /// Spark 9999-12-31 sentinel sits in (1e16, 1e18].
    constexpr Int64 spark_9999_us = 253'402'300'799'999'999LL;
    EXPECT_FALSE(deserializeFieldFromBinaryRepr(littleEndianInt64(spark_9999_us), type, false).has_value());
}

#endif
