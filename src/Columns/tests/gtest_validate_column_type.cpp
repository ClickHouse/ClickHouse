#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/validateColumnType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>

using namespace DB;

/// columnMatchesType is the property fuzzer's validity gate (gtest_functions_stress.cpp).
/// getDataType() reports only the physical TypeIndex (Decimal64 for any DateTime64 scale), so
/// without strict_decimal_scale a scale-7 column passes against a DateTime64(3) type, gets
/// stashed, and is later reused as an argument, surfacing the writeSlice LOGICAL_ERROR in an
/// innocent consumer (issue #108517). strict_decimal_scale rejects it at the producer's gate.

namespace
{

ColumnPtr makeDateTime64Column(UInt32 scale, size_t rows)
{
    auto col = ColumnDecimal<DateTime64>::create(0, scale);
    for (size_t i = 0; i < rows; ++i)
        col->insertDefault();
    return col;
}

ColumnPtr makeDateTime64Array(UInt32 element_scale, size_t elements)
{
    auto data = ColumnDecimal<DateTime64>::create(0, element_scale);
    for (size_t i = 0; i < elements; ++i)
        data->insertDefault();

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert(elements);
    return ColumnArray::create(std::move(data), std::move(offsets));
}

}

TEST(ValidateColumnType, DecimalScaleDivergence)
{
    auto type_dt64_3 = std::make_shared<DataTypeDateTime64>(3);

    auto matching = makeDateTime64Column(3, 4);
    auto divergent = makeDateTime64Column(7, 4);

    /// Scale is ignored by default, so the divergent column passes (engine callers rely on this).
    EXPECT_TRUE(columnMatchesType(*matching, *type_dt64_3));
    EXPECT_TRUE(columnMatchesType(*divergent, *type_dt64_3));

    /// Strict mode compares scale: the matching column still passes, the divergent one is rejected.
    EXPECT_TRUE(columnMatchesType(*matching, *type_dt64_3, /*strict_decimal_scale=*/ true));
    EXPECT_FALSE(columnMatchesType(*divergent, *type_dt64_3, /*strict_decimal_scale=*/ true));
}

TEST(ValidateColumnType, DecimalScaleDivergenceNested)
{
    /// The divergence reaches the gate nested inside Array(DateTime64(3)) (the exact #108517 shape).
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime64>(3));

    auto matching_array = makeDateTime64Array(3, 2);
    auto divergent_array = makeDateTime64Array(7, 2);

    EXPECT_TRUE(columnMatchesType(*matching_array, *array_type));
    EXPECT_TRUE(columnMatchesType(*divergent_array, *array_type));

    EXPECT_TRUE(columnMatchesType(*matching_array, *array_type, /*strict_decimal_scale=*/ true));
    EXPECT_FALSE(columnMatchesType(*divergent_array, *array_type, /*strict_decimal_scale=*/ true));
}
