#include <gtest/gtest.h>
#include <Functions/FunctionsConversion.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;
using namespace DB::detail;

TEST(ConversionMonotonic, toNumber)
{
    /// From_type and to_type are the same
    ASSERT_EQ(ToNumberMonotonicity<UInt32>::get(DataTypeUInt32(), {}, {}).is_strict, true);
    /// Size of to_type is expanded
    ASSERT_EQ(ToNumberMonotonicity<UInt64>::get(DataTypeUInt32(), {}, {}).is_strict, true);
    /// Size of to_type is expanded, and to_type is signed
    ASSERT_EQ(ToNumberMonotonicity<Int64>::get(DataTypeUInt32(), {}, {}).is_strict, true);
    /// Size of to_type is shrunk
    ASSERT_EQ(ToNumberMonotonicity<UInt32>::get(DataTypeUInt64(), {}, {}).is_strict, false);

    /// To_type is float, and the range of from_type exceeds the range of to_type
    ASSERT_EQ(ToNumberMonotonicity<BFloat16>::get(DataTypeUInt16(), {}, {}).is_strict, false);
    ASSERT_EQ(ToNumberMonotonicity<Float32>::get(DataTypeUInt32(), {}, {}).is_strict, false);
    ASSERT_EQ(ToNumberMonotonicity<Float64>::get(DataTypeUInt64(), {}, {}).is_strict, false);

    /// To_type is float, and the range of from_type is in the range of to_type
    ASSERT_EQ(ToNumberMonotonicity<BFloat16>::get(DataTypeUInt8(), {}, {}).is_strict, true);
    ASSERT_EQ(ToNumberMonotonicity<Float32>::get(DataTypeUInt16(), {}, {}).is_strict, true);
    ASSERT_EQ(ToNumberMonotonicity<Float64>::get(DataTypeUInt32(), {}, {}).is_strict, true);
}

TEST(ConversionMonotonic, toDate)
{
    /// The range of from_type is in the range of to_type
    ASSERT_EQ(ToDateMonotonicity<DataTypeDate>::get(DataTypeUInt16(), {}, {}).is_strict, true);
    ASSERT_EQ(ToDateMonotonicity<DataTypeDate32>::get(DataTypeInt16(), {}, {}).is_strict, true);

    /// The range of from_type exceeds the range of to_type
    ASSERT_EQ(ToDateMonotonicity<DataTypeDate>::get(DataTypeUInt32(), {}, {}).is_strict, false);
    ASSERT_EQ(ToDateMonotonicity<DataTypeDate32>::get(DataTypeInt64(), {}, {}).is_strict, false);
}

TEST(ConversionMonotonic, toDateTime)
{
    /// The range of from_type is in the range of to_type
    ASSERT_EQ(ToDateTimeMonotonicity<DataTypeDateTime>::get(DataTypeUInt16(), {}, {}).is_strict, true);
    ASSERT_EQ(ToDateTimeMonotonicity<DataTypeDateTime64>::get(DataTypeInt16(), {}, {}).is_strict, true);

    /// The range of from_type exceeds the range of to_type
    ASSERT_EQ(ToDateTimeMonotonicity<DataTypeDateTime>::get(DataTypeUInt64(), {}, {}).is_strict, false);
}
