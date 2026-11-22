#pragma once

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/IDataType.h>
#include <Common/assert_cast.h>
#include <base/arithmeticOverflow.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

/** Exact reading and scaling of the numbers the KQL timespan functions take.
  *
  * A timespan is an `Interval` of integer ticks, and Kusto scales it by an arbitrary number.
  * Going through `Float64` loses the low digits of a `Decimal` and every unit above `2^53` of
  * a wide integer, which turns an exact duration into one that is a tick or two short. So a
  * value of an exact type is read as its unscaled integer at full width and scaled in `Int256`.
  */
namespace KQLExact
{

/// Whether `unscaledValue` reads a value of this type exactly. Floats are not exact.
inline bool isExactNumber(const IDataType & type)
{
    return isInteger(type) || isDecimal(type);
}

/// The unscaled integer value of an integer of any width, or of a decimal: `12.34` with
/// scale 2 reads as `1234`.
inline Int256 unscaledValue(const IColumn & column, const IDataType & type, size_t row)
{
    switch (WhichDataType(type).idx)
    {
        case TypeIndex::Decimal32:
            return Int256(assert_cast<const ColumnDecimal<Decimal32> &>(column).getData()[row].value);
        case TypeIndex::Decimal64:
            return Int256(assert_cast<const ColumnDecimal<Decimal64> &>(column).getData()[row].value);
        case TypeIndex::Decimal128:
            return Int256(assert_cast<const ColumnDecimal<Decimal128> &>(column).getData()[row].value);
        case TypeIndex::Decimal256:
            return assert_cast<const ColumnDecimal<Decimal256> &>(column).getData()[row].value;
        case TypeIndex::Int128:
            return Int256(assert_cast<const ColumnInt128 &>(column).getData()[row]);
        case TypeIndex::Int256:
            return assert_cast<const ColumnInt256 &>(column).getData()[row];
        case TypeIndex::UInt128:
            return Int256(assert_cast<const ColumnUInt128 &>(column).getData()[row]);
        case TypeIndex::UInt256:
        {
            const UInt256 value = assert_cast<const ColumnUInt256 &>(column).getData()[row];
            if (value > UInt256(std::numeric_limits<Int256>::max()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "A UInt256 value above the largest Int256 cannot be counted exactly");
            return Int256(value);
        }
        default:
            return isNativeUInt(type) ? Int256(column.getUInt(row)) : Int256(column.getInt(row));
    }
}

/// The decimal scale of `type`; an integer has none.
inline UInt32 scaleOf(const IDataType & type)
{
    return isDecimal(type) ? getDecimalScale(type) : 0;
}

inline Int256 powerOfTen(UInt32 exponent)
{
    Int256 result = 1;
    for (UInt32 i = 0; i < exponent; ++i)
        result *= 10;
    return result;
}

/// `ticks * value` where `value` is the row's exact number, truncated toward zero the way
/// the `Float64` path's `trunc` does, and required to fit a timespan.
inline Int64 scaledTicks(Int64 ticks, const IColumn & column, const IDataType & type, size_t row, std::string_view function_name)
{
    Int256 product;
    if (common::mulOverflow(Int256(ticks), unscaledValue(column, type, row), product))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} result does not fit a timespan", function_name);

    const Int256 result = product / powerOfTen(scaleOf(type));
    if (result > Int256(std::numeric_limits<Int64>::max()) || result < Int256(std::numeric_limits<Int64>::min()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} result does not fit a timespan", function_name);
    return static_cast<Int64>(result);
}

}

}
