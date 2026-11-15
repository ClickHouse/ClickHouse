#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/validateColumnType.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

namespace DB::ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

using namespace DB;

/// reinterpret(x, 'Decimal128(scale)') keys executeImpl on the physical TypeIndex, so a source of
/// the same physical width and a different scale (e.g. Decimal(38, 33) -> Decimal128(2)) hits the
/// std::is_same_v<FromType, ToType> branch. That branch must not return the source column verbatim,
/// because its scale (33) would diverge from the declared result_type scale (2), leaving a
/// structurally inconsistent ColumnDecimal. Found by the function property fuzzer once #108551 added
/// the strict scale gate; reinterpret was the second producer of such a column.

namespace
{

template <typename T>
ColumnPtr makeOneRowDecimal(UInt32 scale, typename T::NativeType raw)
{
    auto col = ColumnDecimal<T>::create(0, scale);
    col->insertValue(T(raw));
    return col;
}

/// Run reinterpret(value, target_type_name) over a one-row Decimal column and return the result.
ColumnPtr runReinterpret(const DataTypePtr & from_type, ColumnPtr from_column, const String & target_type_name)
{
    tryRegisterFunctions();
    auto context = getContext().context;

    ColumnPtr type_const = DataTypeString().createColumnConst(1, target_type_name);
    ColumnWithTypeAndName from_arg(from_column, from_type, "x");
    ColumnWithTypeAndName type_arg(type_const, std::make_shared<DataTypeString>(), "type");

    ColumnsWithTypeAndName args{from_arg, type_arg};

    auto function = FunctionFactory::instance().get("reinterpret", context)->build(args);
    auto result_type = function->getResultType();
    auto result = function->execute(args, result_type, /*input_rows_count=*/ 1, /*dry_run=*/ false);

    /// columnMatchesType is scale-blind; each test asserts the declared scale via getScale().
    EXPECT_TRUE(columnMatchesType(*result, *result_type))
        << "reinterpret returned " << result->getName() << " for declared type " << result_type->getName();
    return result;
}

}

TEST(ReinterpretDecimalScale, SameWidthDifferentScale)
{
    /// Decimal(38, 33) -> Decimal128(2): same physical type (Decimal128), declared scale must be 2.
    auto from_type = std::make_shared<DataTypeDecimal<Decimal128>>(38, 33);
    auto result = runReinterpret(from_type, makeOneRowDecimal<Decimal128>(33, Int128(123)), "Decimal128(2)");

    const auto & col_res = assert_cast<const ColumnDecimal<Decimal128> &>(*result);
    EXPECT_EQ(col_res.getScale(), 2u);
    /// Raw value is reinterpreted (bits preserved), only the scale label changes.
    EXPECT_EQ(col_res.getData()[0].value, Int128(123));
}

TEST(ReinterpretDecimalScale, Decimal64SameWidthDifferentScale)
{
    auto from_type = std::make_shared<DataTypeDecimal<Decimal64>>(18, 10);
    auto result = runReinterpret(from_type, makeOneRowDecimal<Decimal64>(10, Int64(456)), "Decimal64(3)");

    const auto & col_res = assert_cast<const ColumnDecimal<Decimal64> &>(*result);
    EXPECT_EQ(col_res.getScale(), 3u);
    EXPECT_EQ(col_res.getData()[0].value, Int64(456));
}

TEST(ReinterpretDecimalScale, Decimal256SameWidthDifferentScale)
{
    auto from_type = std::make_shared<DataTypeDecimal<Decimal256>>(76, 50);
    auto result = runReinterpret(from_type, makeOneRowDecimal<Decimal256>(50, Int256(789)), "Decimal256(4)");

    const auto & col_res = assert_cast<const ColumnDecimal<Decimal256> &>(*result);
    EXPECT_EQ(col_res.getScale(), 4u);
    EXPECT_EQ(col_res.getData()[0].value, Int256(789));
}

TEST(ReinterpretDecimalScale, SameWidthSameScalePreservesColumn)
{
    /// When the scales already match, the column is returned as-is (no rebuild).
    auto from_type = std::make_shared<DataTypeDecimal<Decimal128>>(38, 2);
    auto result = runReinterpret(from_type, makeOneRowDecimal<Decimal128>(2, Int128(321)), "Decimal128(2)");

    const auto & col_res = assert_cast<const ColumnDecimal<Decimal128> &>(*result);
    EXPECT_EQ(col_res.getScale(), 2u);
    EXPECT_EQ(col_res.getData()[0].value, Int128(321));
}

TEST(ReinterpretDecimalScale, DateTime64SameWidthDifferentScale)
{
    /// DateTime64 satisfies IsDataTypeDecimal and is a reachable reinterpret target
    /// (canBeReinterpretedAsNumeric accepts isDateTime64), so DateTime64(9) -> DateTime64(2)
    /// exercises the same scale-rebuild branch as the Decimal cases.
    auto from_type = std::make_shared<DataTypeDateTime64>(9);
    auto result = runReinterpret(from_type, makeOneRowDecimal<DateTime64>(9, Int64(1700000000123456789LL)), "DateTime64(2)");

    const auto & col_res = assert_cast<const ColumnDecimal<DateTime64> &>(*result);
    EXPECT_EQ(col_res.getScale(), 2u);
    EXPECT_EQ(col_res.getData()[0].value, Int64(1700000000123456789LL));
}

TEST(ReinterpretDecimalScale, Time64IsNotAReinterpretTarget)
{
    /// Time64 also satisfies IsDataTypeDecimal, but unlike DateTime64 it is not in the
    /// canBeReinterpretedAsNumeric gate, so reinterpret rejects it in getReturnTypeImpl
    /// before the scale-rebuild branch is ever reached. Document that it throws.
    auto from_type = std::make_shared<DataTypeDecimal<Decimal64>>(18, 9);
    try
    {
        runReinterpret(from_type, makeOneRowDecimal<Decimal64>(9, Int64(1)), "Time64(2)");
        FAIL() << "reinterpret to Time64 was expected to throw ILLEGAL_TYPE_OF_ARGUMENT";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT) << e.message();
    }
}
