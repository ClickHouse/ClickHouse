#pragma once

#include <base/memcmpSmall.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <DataTypes/IDataType.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


template <bool negative = false>
struct EmptyImpl
{
    /// If the function will return constant value for FixedString data type.
    static constexpr auto is_fixed_to_constant = false;

    static constexpr bool has_information_about_monotonicity = true;

    /// For String type, the empty string '' is the minimum value in lexicographic order.
    /// empty(s) = 1 iff s == ''. The function is constant (and thus monotone) on any
    /// range that does not include '', i.e. where the left boundary is non-empty, or on
    /// the degenerate range ['', ''].
    static IFunction::Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right)
    {
        if (!isString(type))
            return {};

        if (!left.isNull() && left.getType() == Field::Types::String)
        {
            const String & left_str = left.safeGet<String>();
            if (!left_str.empty())
            {
                /// All values in [left, right] are non-empty strings.
                /// empty(s) = 0 everywhere, notEmpty(s) = 1 everywhere → constant.
                return {.is_monotonic = true, .is_positive = true};
            }

            /// left == ''
            if (!right.isNull() && right.getType() == Field::Types::String && right.safeGet<String>().empty())
            {
                /// Range is exactly ['', ''] → empty(s) = 1 everywhere, constant.
                return {.is_monotonic = true, .is_positive = true};
            }
        }

        return {};
    }

    static void vector(const ColumnString::Chars & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res, size_t input_rows_count)
    {
        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset next_offset = offsets[i];
            res[i] = negative ^ (next_offset == prev_offset);
            prev_offset = next_offset;
        }
    }

    /// Only make sense if is_fixed_to_constant.
    static void vectorFixedToConstant(const ColumnString::Chars &, size_t, UInt8 &, size_t)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "'vectorFixedToConstant method' is called");
    }

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<UInt8> & res, size_t input_rows_count)
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            res[i] = negative ^ memoryIsZeroSmallAllowOverflow15(data.data() + i * n, n);
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res, size_t input_rows_count)
    {
        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = negative ^ (offsets[i] == prev_offset);
            prev_offset = offsets[i];
        }
    }

    static void uuid(const ColumnUUID::Container & container, size_t n, PaddedPODArray<UInt8> & res, size_t)
    {
        for (size_t i = 0; i < n; ++i)
            res[i] = negative ^ (container[i].toUnderType() == 0);
    }

    static void ipv6(const ColumnIPv6::Container & container, size_t n, PaddedPODArray<UInt8> & res, size_t)
    {
        for (size_t i = 0; i < n; ++i)
            res[i] = negative ^ (container[i].toUnderType() == 0);
    }

    static void ipv4(const ColumnIPv4::Container & container, size_t n, PaddedPODArray<UInt8> & res, size_t)
    {
        for (size_t i = 0; i < n; ++i)
            res[i] = negative ^ (container[i].toUnderType() == 0);
    }
};

}
