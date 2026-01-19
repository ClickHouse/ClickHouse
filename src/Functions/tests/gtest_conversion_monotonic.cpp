#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/RangeRef.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionsConversion.h>

using namespace DB;
using namespace DB::detail;

namespace
{

void expectMonotonicity(const IFunctionBase::Monotonicity & actual, const IFunctionBase::Monotonicity & expected)
{
    EXPECT_EQ(actual.is_monotonic, expected.is_monotonic);
    EXPECT_EQ(actual.is_always_monotonic, expected.is_always_monotonic);

    if (expected.is_monotonic && actual.is_monotonic)
    {
        EXPECT_EQ(actual.is_positive, expected.is_positive);
        EXPECT_EQ(actual.is_strict, expected.is_strict);
    }
}

template <typename ColumnT>
ColumnPtr makeNumericColumn(std::initializer_list<typename ColumnT::ValueType> values)
{
    auto col = ColumnT::create();
    auto & data = col->getData();
    data.reserve(values.size());
    for (const auto & v : values)
        data.push_back(v);
    return col;
}

}

TEST(ConversionMonotonic, toNumber)
{
    /// From_type and to_type are the same
    ASSERT_EQ(ToNumberMonotonicity<UInt32>::get(DataTypeUInt32(), Field{}, Field{}).is_strict, true);
    /// Size of to_type is expanded
    ASSERT_EQ(ToNumberMonotonicity<UInt64>::get(DataTypeUInt32(), Field{}, Field{}).is_strict, true);
    /// Size of to_type is expanded, and to_type is signed
    ASSERT_EQ(ToNumberMonotonicity<Int64>::get(DataTypeUInt32(), Field{}, Field{}).is_strict, true);
    /// Size of to_type is shrunk
    ASSERT_EQ(ToNumberMonotonicity<UInt32>::get(DataTypeUInt64(), Field{}, Field{}).is_strict, false);

    /// To_type is float, and the range of from_type exceeds the range of to_type
    ASSERT_EQ(ToNumberMonotonicity<BFloat16>::get(DataTypeUInt16(), Field{}, Field{}).is_strict, false);
    ASSERT_EQ(ToNumberMonotonicity<Float32>::get(DataTypeUInt32(), Field{}, Field{}).is_strict, false);
    ASSERT_EQ(ToNumberMonotonicity<Float64>::get(DataTypeUInt64(), Field{}, Field{}).is_strict, false);

    /// To_type is float, and the range of from_type is in the range of to_type
    ASSERT_EQ(ToNumberMonotonicity<BFloat16>::get(DataTypeUInt8(), Field{}, Field{}).is_strict, true);
    ASSERT_EQ(ToNumberMonotonicity<Float32>::get(DataTypeUInt16(), Field{}, Field{}).is_strict, true);
    ASSERT_EQ(ToNumberMonotonicity<Float64>::get(DataTypeUInt32(), Field{}, Field{}).is_strict, true);
}

TEST(ConversionMonotonic, toDate)
{
    /// The range of from_type is in the range of to_type
    ASSERT_EQ(ToDateMonotonicity<DataTypeDate>::get(DataTypeUInt16(), Field{}, Field{}).is_strict, true);
    ASSERT_EQ(ToDateMonotonicity<DataTypeDate32>::get(DataTypeInt16(), Field{}, Field{}).is_strict, true);

    /// The range of from_type exceeds the range of to_type
    ASSERT_EQ(ToDateMonotonicity<DataTypeDate>::get(DataTypeUInt32(), Field{}, Field{}).is_strict, false);
    ASSERT_EQ(ToDateMonotonicity<DataTypeDate32>::get(DataTypeInt64(), Field{}, Field{}).is_strict, false);
}

TEST(ConversionMonotonic, toDateTime)
{
    /// The range of from_type is in the range of to_type
    ASSERT_EQ(ToDateTimeMonotonicity<DataTypeDateTime>::get(DataTypeUInt16(), Field{}, Field{}).is_strict, true);
    ASSERT_EQ(ToDateTimeMonotonicity<DataTypeDateTime64>::get(DataTypeInt16(), Field{}, Field{}).is_strict, true);

    /// The range of from_type exceeds the range of to_type
    ASSERT_EQ(ToDateTimeMonotonicity<DataTypeDateTime>::get(DataTypeUInt64(), Field{}, Field{}).is_strict, false);
}

TEST(ConversionMonotonicRef, PositiveAndUnknown)
{
    expectMonotonicity(
        PositiveMonotonicity::get(DataTypeUInt8(), ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity()),
        {.is_monotonic = true});
    expectMonotonicity(
        UnknownMonotonicity::get(DataTypeUInt8(), ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity()), {});
}

TEST(ConversionMonotonicRef, toNumber)
{
    {
        auto col = makeNumericColumn<ColumnUInt64>({0, 0xFFFFFFFFULL});
        expectMonotonicity(
            ToNumberMonotonicity<UInt32>::get(DataTypeUInt64(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {.is_monotonic = true});
    }

    {
        auto col = makeNumericColumn<ColumnUInt64>({0, 0x1'0000'0000ULL});
        expectMonotonicity(
            ToNumberMonotonicity<UInt32>::get(DataTypeUInt64(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {});
    }

    {
        auto col = makeNumericColumn<ColumnFloat64>({-1e9, 1e9});
        expectMonotonicity(
            ToNumberMonotonicity<Int32>::get(DataTypeFloat64(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {.is_monotonic = true});
    }

    {
        auto col = makeNumericColumn<ColumnFloat64>({-1e12, 1e12});
        expectMonotonicity(
            ToNumberMonotonicity<Int32>::get(DataTypeFloat64(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {});
    }

    {
        expectMonotonicity(
            ToNumberMonotonicity<UInt32>::get(DataTypeUInt64(), ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity()),
            {});
    }
}

TEST(ConversionMonotonicRef, toDate)
{
    {
        auto col = makeNumericColumn<ColumnUInt32>({0, 65534});
        expectMonotonicity(
            ToDateMonotonicity<DataTypeDate>::get(
                DataTypeUInt32(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {.is_monotonic = true, .is_always_monotonic = true});
    }

    {
        auto col = makeNumericColumn<ColumnUInt32>({0, 65535});
        expectMonotonicity(
            ToDateMonotonicity<DataTypeDate>::get(
                DataTypeUInt32(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {});
    }

    {
        auto col = makeNumericColumn<ColumnUInt16>({0, 65535});
        expectMonotonicity(
            ToDateMonotonicity<DataTypeDate>::get(
                DataTypeUInt16(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {.is_monotonic = true, .is_always_monotonic = true, .is_strict = true});
    }
}

TEST(ConversionMonotonicRef, toDateTime)
{
    {
        auto col = makeNumericColumn<ColumnUInt16>({0, 1});
        expectMonotonicity(
            ToDateTimeMonotonicity<DataTypeDateTime>::get(
                DataTypeUInt16(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {.is_monotonic = true, .is_always_monotonic = true, .is_strict = true});
    }

    {
        auto col = makeNumericColumn<ColumnUInt64>({0, 1});
        expectMonotonicity(
            ToDateTimeMonotonicity<DataTypeDateTime>::get(
                DataTypeUInt64(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {.is_monotonic = true, .is_always_monotonic = true, .is_strict = false});
    }

    {
        auto col = makeNumericColumn<ColumnUInt8>({0, 1});
        expectMonotonicity(
            ToDateTimeMonotonicity<DataTypeDateTime>::get(
                DataTypeString(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {});
    }
}

TEST(ConversionMonotonicRef, toString)
{
    {
        auto col = makeNumericColumn<ColumnUInt64>({10, 99});
        expectMonotonicity(
            ToStringMonotonicity::get(DataTypeUInt64(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {.is_monotonic = true});
    }

    {
        auto col = makeNumericColumn<ColumnUInt64>({9, 10});
        expectMonotonicity(
            ToStringMonotonicity::get(DataTypeUInt64(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)), {});
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({10, 99});
        expectMonotonicity(
            ToStringMonotonicity::get(DataTypeInt64(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)),
            {.is_monotonic = true});
    }

    {
        auto col = makeNumericColumn<ColumnInt64>({-10, -1});
        expectMonotonicity(
            ToStringMonotonicity::get(DataTypeInt64(), ColumnValueRef::normal(col.get(), 0), ColumnValueRef::normal(col.get(), 1)), {});
    }

    {
        expectMonotonicity(
            ToStringMonotonicity::get(DataTypeUInt64(), ColumnValueRef::negativeInfinity(), ColumnValueRef::positiveInfinity()), {});
    }
}
