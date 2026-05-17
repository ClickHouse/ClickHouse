#pragma once

// Include this first, because `#define _asan_poison_address` from
// llvm/Support/Compiler.h conflicts with its forward declaration in
// sanitizer/asan_interface.h
#include <memory>
#include <type_traits>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Native.h>
#include <DataTypes/NumberTraits.h>
#include <Formats/FormatSettings.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/DivisionUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/IsOperation.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/castColumn.h>
#include <base/TypeList.h>
#include <base/TypeLists.h>
#include <base/types.h>
#include <base/wide_integer_to_string.h>
#include <Common/Arena.h>
#include <Core/AccurateComparison.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <absl/container/inlined_vector.h>

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#endif

#include <cassert>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int DECIMAL_OVERFLOW;
    extern const int CANNOT_ADD_DIFFERENT_AGGREGATE_STATES;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

FormatSettings::DateTimeOverflowBehavior getDateTimeOverflowBehavior(ContextPtr context);

namespace traits_
{
struct InvalidType; /// Used to indicate undefined operation

template <bool V, typename T> struct Case : std::bool_constant<V> { using type = T; };

/// Switch<Case<C0, T0>, ...> -- select the first Ti for which Ci is true, InvalidType if none.
template <typename... Ts> using Switch = typename std::disjunction<Ts..., Case<true, InvalidType>>::type;

template <class T>
using DataTypeFromFieldType = std::conditional_t<std::is_same_v<T, NumberTraits::Error>,
    InvalidType, DataTypeNumber<T>>;

template <typename DataType> constexpr bool IsIntegral = false;
template <> inline constexpr bool IsIntegral<DataTypeUInt8> = true;
template <> inline constexpr bool IsIntegral<DataTypeUInt16> = true;
template <> inline constexpr bool IsIntegral<DataTypeUInt32> = true;
template <> inline constexpr bool IsIntegral<DataTypeUInt64> = true;
template <> inline constexpr bool IsIntegral<DataTypeInt8> = true;
template <> inline constexpr bool IsIntegral<DataTypeInt16> = true;
template <> inline constexpr bool IsIntegral<DataTypeInt32> = true;
template <> inline constexpr bool IsIntegral<DataTypeInt64> = true;

template <typename DataType> constexpr bool IsExtended = false;
template <> inline constexpr bool IsExtended<DataTypeUInt128> = true;
template <> inline constexpr bool IsExtended<DataTypeUInt256> = true;
template <> inline constexpr bool IsExtended<DataTypeInt128> = true;
template <> inline constexpr bool IsExtended<DataTypeInt256> = true;

template <typename DataType> constexpr bool IsIntegralOrExtended = IsIntegral<DataType> || IsExtended<DataType>;
template <typename DataType> constexpr bool IsIntegralOrExtendedOrDecimal =
    IsIntegralOrExtended<DataType> ||
    IsDataTypeDecimal<DataType>;

template <typename DataType> constexpr bool IsFloatingPoint = false;
template <> inline constexpr bool IsFloatingPoint<DataTypeBFloat16> = true;
template <> inline constexpr bool IsFloatingPoint<DataTypeFloat32> = true;
template <> inline constexpr bool IsFloatingPoint<DataTypeFloat64> = true;

template <typename DataType> constexpr bool IsArray = false;
template <> inline constexpr bool IsArray<DataTypeArray> = true;

template <typename DataType> constexpr bool IsDateOrDateTime = false;
template <> inline constexpr bool IsDateOrDateTime<DataTypeDate> = true;
template <> inline constexpr bool IsDateOrDateTime<DataTypeDateTime> = true;

template <typename DataType> constexpr bool IsDateOrTimeOrDateTime = false;
template <> inline constexpr bool IsDateOrTimeOrDateTime<DataTypeDate> = true;
template <> inline constexpr bool IsDateOrTimeOrDateTime<DataTypeTime> = true;
template <> inline constexpr bool IsDateOrTimeOrDateTime<DataTypeDateTime> = true;

template <typename DataType> constexpr bool IsIPv4 = false;
template <> inline constexpr bool IsIPv4<DataTypeIPv4> = true;

template <typename T0, typename T1> constexpr bool UseLeftDecimal = false;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal256>, DataTypeDecimal<Decimal128>> = true;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal256>, DataTypeDecimal<Decimal64>> = true;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal256>, DataTypeDecimal<Decimal32>> = true;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal32>> = true;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal64>> = true;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal64>, DataTypeDecimal<Decimal32>> = true;

template <typename DataType> constexpr bool IsFixedString = false;
template <> inline constexpr bool IsFixedString<DataTypeFixedString> = true;

template <typename DataType> constexpr bool IsString = false;
template <> inline constexpr bool IsString<DataTypeString> = true;

template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct BinaryOperationTraits
{
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
private: /// it's not correct for Decimal
    using Op = Operation<T0, T1>;

public:
    static constexpr bool allow_decimal = IsOperation<Operation>::allow_decimal;

    using DecimalResultDataType = Switch<
        Case<!allow_decimal, InvalidType>,
        Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> && UseLeftDecimal<LeftDataType, RightDataType>, LeftDataType>,
        Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType>, RightDataType>,
        Case<IsDataTypeDecimal<LeftDataType> && IsIntegralOrExtended<RightDataType>, LeftDataType>,
        Case<IsDataTypeDecimal<RightDataType> && IsIntegralOrExtended<LeftDataType>, RightDataType>,

        /// e.g Decimal +-*/ Float, least(Decimal, Float), greatest(Decimal, Float) = Float64
        Case<IsDataTypeDecimal<LeftDataType> && IsFloatingPoint<RightDataType>, DataTypeFloat64>,
        Case<IsDataTypeDecimal<RightDataType> && IsFloatingPoint<LeftDataType>, DataTypeFloat64>,

        Case<IsOperation<Operation>::bit_hamming_distance && IsIntegral<LeftDataType> && IsIntegral<RightDataType>, DataTypeUInt8>,
        Case<IsOperation<Operation>::bit_hamming_distance && IsFixedString<LeftDataType> && IsFixedString<RightDataType>, DataTypeUInt16>,
        Case<IsOperation<Operation>::bit_hamming_distance && IsString<LeftDataType> && IsString<RightDataType>, DataTypeUInt64>,

          /// Decimal <op> Real is not supported (traditional DBs convert Decimal <op> Real to Real)
        Case<IsDataTypeDecimal<LeftDataType> && !IsIntegralOrExtendedOrDecimal<RightDataType>, InvalidType>,
        Case<IsDataTypeDecimal<RightDataType> && !IsIntegralOrExtendedOrDecimal<LeftDataType>, InvalidType>>;

    /// Appropriate result type for binary operator on numeric types. "Date" can also mean
    /// DateTime, but if both operands are Dates, their type must be the same (e.g. Date - DateTime is invalid).
    using ResultDataType = Switch<
        /// Result must be Integer
        Case<IsOperation<Operation>::int_div || IsOperation<Operation>::int_div_or_zero,
            std::conditional_t<IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>, DataTypeFromFieldType<typename Op::ResultType>, InvalidType>>,
        /// Decimal cases
        Case<IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>, DecimalResultDataType>,
        Case<
            IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> && UseLeftDecimal<LeftDataType, RightDataType>,
            LeftDataType>,
        Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType>, RightDataType>,
        Case<IsDataTypeDecimal<LeftDataType> && IsIntegralOrExtended<RightDataType>, LeftDataType>,
        Case<IsDataTypeDecimal<RightDataType> && IsIntegralOrExtended<LeftDataType>, RightDataType>,

        /// e.g Decimal +-*/ Float, least(Decimal, Float), greatest(Decimal, Float) = Float64
        Case<IsOperation<Operation>::allow_decimal && IsDataTypeDecimal<LeftDataType> && IsFloatingPoint<RightDataType>, DataTypeFloat64>,
        Case<IsOperation<Operation>::allow_decimal && IsDataTypeDecimal<RightDataType> && IsFloatingPoint<LeftDataType>, DataTypeFloat64>,

        Case<IsOperation<Operation>::bit_hamming_distance && IsIntegral<LeftDataType> && IsIntegral<RightDataType>, DataTypeUInt8>,
        Case<IsOperation<Operation>::bit_hamming_distance && IsFixedString<LeftDataType> && IsFixedString<RightDataType>, DataTypeUInt16>,
        Case<IsOperation<Operation>::bit_hamming_distance && IsString<LeftDataType> && IsString<RightDataType>, DataTypeUInt64>,

        /// Decimal <op> Real is not supported (traditional DBs convert Decimal <op> Real to Real)
        Case<IsDataTypeDecimal<LeftDataType> && !IsIntegralOrExtendedOrDecimal<RightDataType>, InvalidType>,
        Case<IsDataTypeDecimal<RightDataType> && !IsIntegralOrExtendedOrDecimal<LeftDataType>, InvalidType>,

        /// number <op> number -> see corresponding impl
        Case<!IsDateOrTimeOrDateTime<LeftDataType> && !IsDateOrTimeOrDateTime<RightDataType>, DataTypeFromFieldType<typename Op::ResultType>>,

        /// Date + Integral -> Date
        /// Integral + Date -> Date
        Case<
            IsOperation<Operation>::plus,
            Switch<Case<IsIntegral<RightDataType>, LeftDataType>, Case<IsIntegral<LeftDataType>, RightDataType>>>,

        /// Date - Date     -> Int32
        /// Date - Integral -> Date
        Case<
            IsOperation<Operation>::minus,
            Switch<
                Case<std::is_same_v<LeftDataType, RightDataType>, DataTypeInt32>,
                Case<IsDateOrTimeOrDateTime<LeftDataType> && IsIntegral<RightDataType>, LeftDataType>>>,

        /// least(Date, Date) -> Date
        /// greatest(Date, Date) -> Date
        Case<
            std::is_same_v<LeftDataType, RightDataType> && (IsOperation<Operation>::least || IsOperation<Operation>::greatest),
            LeftDataType>,

        /// Date % Int32 -> Int32
        /// Date % Float -> Float64
        Case<
            IsOperation<Operation>::modulo || IsOperation<Operation>::positive_modulo,
            Switch<
                Case<IsDateOrTimeOrDateTime<LeftDataType> && IsIntegral<RightDataType>, RightDataType>,
                Case<IsDateOrTimeOrDateTime<LeftDataType> && IsFloatingPoint<RightDataType>, DataTypeFloat64>>>>;
};
}

namespace impl_
{

template <bool is_multiply, bool is_division, typename T, typename U, template <typename> typename DecimalType>
inline auto decimalResultType(const DecimalType<T> & tx, const DecimalType<U> & ty)
{
    const auto result_trait = DecimalUtils::binaryOpResult<is_multiply, is_division>(tx, ty);
    return DecimalType<typename decltype(result_trait)::FieldType>(result_trait.precision, result_trait.scale);
}

template <bool is_multiply, bool is_division, typename T, typename U, template <typename> typename DecimalType>
inline DecimalType<T> decimalResultType(const DecimalType<T> & tx, const DataTypeNumber<U> & ty)
{
    const auto result_trait = DecimalUtils::binaryOpResult<is_multiply, is_division>(tx, ty);
    return DecimalType<typename decltype(result_trait)::FieldType>(result_trait.precision, result_trait.scale);
}

template <bool is_multiply, bool is_division, typename T, typename U, template <typename> typename DecimalType>
inline DecimalType<U> decimalResultType(const DataTypeNumber<T> & tx, const DecimalType<U> & ty)
{
    const auto result_trait = DecimalUtils::binaryOpResult<is_multiply, is_division>(tx, ty);
    return DecimalType<typename decltype(result_trait)::FieldType>(result_trait.precision, result_trait.scale);
}

/** Arithmetic operations: +, -, *, /, %,
  * intDiv (integer division)
  * Bitwise operations: |, &, ^, ~.
  * Etc.
  */

enum class OpCase : uint8_t
{
    Vector,
    LeftConstant,
    RightConstant
};

constexpr const auto & undec(const auto & x) { return x; }
constexpr const auto & undec(const is_decimal auto & x) { return x.value; }

template <typename A, typename B, typename Op, typename OpResultType = typename Op::ResultType>
struct BinaryOperation
{
    using ResultType = OpResultType;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <OpCase op_case>
    static void NO_INLINE process(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t size, const NullMap * right_nullmap = nullptr)
    {
        if constexpr (op_case == OpCase::RightConstant)
        {
            if (right_nullmap && (*right_nullmap)[0])
                return;

            for (size_t i = 0; i < size; ++i)
                c[i] = Op::template apply<ResultType>(a[i], *b);
        }
        else
        {
            if (right_nullmap)
            {
                for (size_t i = 0; i < size; ++i)
                    if ((*right_nullmap)[i])
                        c[i] = ResultType();
                    else
                        apply<op_case>(a, b, c, i);
            }
            else
                for (size_t i = 0; i < size; ++i)
                    apply<op_case>(a, b, c, i);
        }
    }

    static ResultType process(A a, B b) { return Op::template apply<ResultType>(a, b); }

private:
    template <OpCase op_case>
    static void apply(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t i)
    {
        if constexpr (op_case == OpCase::Vector)
            c[i] = Op::template apply<ResultType>(a[i], b[i]);
        else
            c[i] = Op::template apply<ResultType>(*a, b[i]);
    }
};

template <typename B, typename Op>
struct StringIntegerOperationImpl
{
    template <OpCase op_case>
    static void NO_INLINE processFixedString(const UInt8 * __restrict in_vec, const UInt64 n, const B * __restrict b, ColumnFixedString::Chars & out_vec, size_t size)
    {
        size_t prev_offset = 0;
        out_vec.reserve(n * size);
        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (op_case == OpCase::LeftConstant)
            {
                Op::apply(&in_vec[0], &in_vec[n], b[i], out_vec);
            }
            else
            {
                size_t new_offset = prev_offset + n;

                if constexpr (op_case == OpCase::Vector)
                {
                    Op::apply(&in_vec[prev_offset], &in_vec[new_offset], b[i], out_vec);
                }
                else
                {
                    Op::apply(&in_vec[prev_offset], &in_vec[new_offset], b[0], out_vec);
                }
                prev_offset = new_offset;
            }
        }
    }


    template <OpCase op_case>
    static void NO_INLINE processString(const UInt8 * __restrict in_vec, const UInt64 * __restrict in_offsets, const B * __restrict b, ColumnString::Chars & out_vec, ColumnString::Offsets & out_offsets, size_t size)
    {
        size_t prev_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (op_case == OpCase::LeftConstant)
            {
                Op::apply(&in_vec[0], &in_vec[in_offsets[0]], b[i], out_vec, out_offsets);
            }
            else
            {
                size_t new_offset = in_offsets[i];

                if constexpr (op_case == OpCase::Vector)
                {
                    Op::apply(&in_vec[prev_offset], &in_vec[new_offset], b[i], out_vec, out_offsets);
                }
                else
                {
                    Op::apply(&in_vec[prev_offset], &in_vec[new_offset], b[0], out_vec, out_offsets);
                }

                prev_offset = new_offset;
            }
        }
    }
};

template <typename Op>
struct FixedStringOperationImpl
{
    template <OpCase op_case>
    static void NO_INLINE process(
        const UInt8 * __restrict a, const UInt8 * __restrict b, UInt8 * __restrict result,
        size_t size, [[maybe_unused]] size_t N)
    {
        if constexpr (op_case == OpCase::Vector)
            for (size_t i = 0; i < size; ++i)
                result[i] = Op::template apply<UInt8>(a[i], b[i]);
        else if constexpr (op_case == OpCase::LeftConstant)
            withConst<true>(b, a, result, size, N);
        else
            withConst<false>(a, b, result, size, N);
    }

private:
    template <bool inverted>
    static void NO_INLINE withConst(const UInt8 * __restrict a, const UInt8 * __restrict b, UInt8 * __restrict c, size_t size, size_t N)
    {
        /// These complications are needed to avoid integer division in inner loop.

        /// Create a pattern of repeated values of b with at least 16 bytes,
        /// so we can read 16 bytes of this repeated pattern starting from any offset inside b.
        ///
        /// Example:
        ///
        ///  N = 6
        ///  ------
        /// [abcdefabcdefabcdefabc]
        ///       ^^^^^^^^^^^^^^^^
        ///      16 bytes starting from the last offset inside b.

        const size_t b_repeated_size = N + 15;

        absl::InlinedVector<UInt8, 64> b_repeated(b_repeated_size);

        for (size_t i = 0; i < b_repeated_size; ++i)
            b_repeated[i] = b[i % N];

        size_t b_offset = 0;
        const size_t b_increment = 16 % N;

        /// Example:
        ///
        /// At first iteration we copy 16 bytes at offset 0 from b_repeated:
        /// [abcdefabcdefabcdefabc]
        ///  ^^^^^^^^^^^^^^^^
        /// At second iteration we copy 16 bytes at offset 4 = 16 % 6 from b_repeated:
        /// [abcdefabcdefabcdefabc]
        ///      ^^^^^^^^^^^^^^^^
        /// At third iteration we copy 16 bytes at offset 2 = (16 * 2) % 6 from b_repeated:
        /// [abcdefabcdefabcdefabc]
        ///    ^^^^^^^^^^^^^^^^

        /// PaddedPODArray allows overflow for 15 bytes.
        for (size_t i = 0; i < size; i += 16)
        {
            /// This loop is formed in a way to be vectorized into two SIMD mov.
            for (size_t j = 0; j < 16; ++j)
                c[i + j] = inverted
                    ? Op::template apply<UInt8>(a[i + j], b_repeated[b_offset + j])
                    : Op::template apply<UInt8>(b_repeated[b_offset + j], a[i + j]);

            b_offset += b_increment;

            if (b_offset >= N) /// This condition is easily predictable.
                b_offset -= N;
        }
    }
};

template <typename Op>
struct FixedStringReduceOperationImpl
{
    template <OpCase op_case>
    static void process(const UInt8 * __restrict a, const UInt8 * __restrict b, UInt16 * __restrict result, size_t size, size_t N)
    {
        if constexpr (op_case == OpCase::Vector)
            vectorVector(a, b, result, size, N);
        else if constexpr (op_case == OpCase::LeftConstant)
            vectorConstant(b, a, result, size, N);
        else
            vectorConstant(a, b, result, size, N);
    }

private:
    static void vectorVector(const UInt8 * __restrict a, const UInt8 * __restrict b, UInt16 * __restrict result, size_t size, size_t N)
    {
        for (size_t i = 0; i < size; ++i)
        {
            size_t offset = i * N;
            for (size_t j = 0; j < N; ++j)
            {
                result[i] += Op::template apply<UInt8>(a[offset + j], b[offset + j]);
            }
        }
    }

    static void vectorConstant(const UInt8 * __restrict a, const UInt8 * __restrict b, UInt16 * __restrict result, size_t size, size_t N)
    {
        for (size_t i = 0; i < size; ++i)
        {
            size_t offset = i * N;
            for (size_t j = 0; j < N; ++j)
            {
                result[i] += Op::template apply<UInt8>(a[offset + j], b[j]);
            }
        }
    }
};

template <typename Op>
struct StringReduceOperationImpl
{
    static void vectorVector(
        const ColumnString::Chars & a,
        const ColumnString::Offsets & offsets_a,
        const ColumnString::Chars & b,
        const ColumnString::Offsets & offsets_b,
        PaddedPODArray<UInt64> & res)
    {
        size_t size = res.size();
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = process(
                a.data() + offsets_a[i - 1],
                a.data() + offsets_a[i],
                b.data() + offsets_b[i - 1],
                b.data() + offsets_b[i]);
        }
    }

    static void
    vectorConstant(const ColumnString::Chars & a, const ColumnString::Offsets & offsets_a, std::string_view b, PaddedPODArray<UInt64> & res)
    {
        size_t size = res.size();
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = process(
                a.data() + offsets_a[i - 1],
                a.data() + offsets_a[i],
                reinterpret_cast<const UInt8 *>(b.data()),
                reinterpret_cast<const UInt8 *>(b.data()) + b.size());
        }
    }

    static UInt64 constConst(std::string_view a, std::string_view b)
    {
        return process(
            reinterpret_cast<const UInt8 *>(a.data()),
            reinterpret_cast<const UInt8 *>(a.data()) + a.size(),
            reinterpret_cast<const UInt8 *>(b.data()),
            reinterpret_cast<const UInt8 *>(b.data()) + b.size());
    }

private:
    static UInt64 process(const UInt8 * __restrict start_a, const UInt8 * __restrict end_a, const UInt8 * start_b, const UInt8 * end_b)
    {
        UInt64 res = 0;
        while (start_a < end_a && start_b < end_b)
            res += Op::template apply<UInt8>(*start_a++, *start_b++);

        while (start_a < end_a)
            res += Op::template apply<UInt8>(*start_a++, 0);
        while (start_b < end_b)
            res += Op::template apply<UInt8>(0, *start_b++);
        return res;
    }
};

template <typename A, typename B, typename Op, typename ResultType = typename Op::ResultType>
struct BinaryOperationImpl : BinaryOperation<A, B, Op, ResultType> { };

/**
 * Binary operations with Decimals (either Decimal OP Decimal or Decimal Op Float) need to scale the args correctly.
 *  - + (plus), - (minus), * (multiply), least and greatest operations scale one of the args (which scale factor is not 1).
 *    The resulting scale is either left or the right scale.
 *  - / (divide) operation scales the first argument.
 *    The resulting scale is the first one's.
 */
template <template <typename, typename> typename Operation, class OpResultType, bool check_overflow = true>
struct DecimalBinaryOperation
{
private:
    using ResultType = OpResultType; // e.g. Decimal32
    using NativeResultType = NativeType<ResultType>; // e.g. UInt32 for Decimal32

    using ResultContainerType = typename ColumnVectorOrDecimal<ResultType>::Container;

public:
    template <OpCase op_case, bool is_decimal_a, bool is_decimal_b>
    static void NO_INLINE process(const auto & a, const auto & b, ResultContainerType & c,
        NativeResultType scale_a, NativeResultType scale_b, const NullMap * right_nullmap = nullptr)
    {
        if constexpr (op_case == OpCase::LeftConstant) static_assert(!is_decimal<decltype(a)>);
        if constexpr (op_case == OpCase::RightConstant) static_assert(!is_decimal<decltype(b)>);

        size_t size;

        if constexpr (op_case == OpCase::LeftConstant)
            size = b.size();
        else
            size = a.size();

        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(
                        static_cast<NativeResultType>(unwrap<op_case, OpCase::LeftConstant>(a, i)),
                        static_cast<NativeResultType>(unwrap<op_case, OpCase::RightConstant>(b, i)),
                        scale_a);
                return;
            }
            if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(
                        static_cast<NativeResultType>(unwrap<op_case, OpCase::LeftConstant>(a, i)),
                        static_cast<NativeResultType>(unwrap<op_case, OpCase::RightConstant>(b, i)),
                        scale_b);
                return;
            }
        }
        else if constexpr (is_multiply)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true, false>(
                        static_cast<NativeResultType>(unwrap<op_case, OpCase::LeftConstant>(a, i)),
                        static_cast<NativeResultType>(unwrap<op_case, OpCase::RightConstant>(b, i)),
                        scale_a);
                return;
            }
            if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false, false>(
                        static_cast<NativeResultType>(unwrap<op_case, OpCase::LeftConstant>(a, i)),
                        static_cast<NativeResultType>(unwrap<op_case, OpCase::RightConstant>(b, i)),
                        scale_b);
                return;
            }
        }
        else if constexpr (is_division && is_decimal_b)
        {
            processWithRightNullmapImpl<op_case>(a, b, c, size, right_nullmap, [&scale_a](const auto & left, const auto & right)
            {
                return applyScaledDiv<is_decimal_a>(
                    static_cast<NativeResultType>(left), right, scale_a);
            });
            return;
        }

        processWithRightNullmapImpl<op_case>(
            a, b, c, size, right_nullmap,
            [](const auto & left, const auto & right)
            {
                return apply(
                    static_cast<NativeResultType>(left),
                    static_cast<NativeResultType>(right));
            });
    }

    template <bool is_decimal_a, bool is_decimal_b, class A, class B>
    static ResultType process(A a, B b, NativeResultType scale_a, NativeResultType scale_b)
        requires(!is_decimal<A> && !is_decimal<B>)
    {
        if constexpr (is_division && is_decimal_b)
            return applyScaledDiv<is_decimal_a>(a, b, scale_a);
        else if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
                return applyScaled<true>(a, b, scale_a);
            if (scale_b != 1)
                return applyScaled<false>(a, b, scale_b);
        }

        return apply(a, b);
    }

private:
    template <OpCase op_case, typename ApplyFunc>
    static void processWithRightNullmapImpl(const auto & a, const auto & b, ResultContainerType & c, size_t size, const NullMap * right_nullmap, ApplyFunc apply_func)
    {
        if (right_nullmap)
        {
            if constexpr (op_case == OpCase::RightConstant)
            {
                if ((*right_nullmap)[0])
                {
                    for (size_t i = 0; i < size; ++i)
                        c[i] = ResultType();
                    return;
                }

                for (size_t i = 0; i < size; ++i)
                    c[i] = apply_func(undec(a[i]), undec(b));
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                {
                    if ((*right_nullmap)[i])
                        c[i] = ResultType();
                    else
                        c[i] = apply_func(unwrap<op_case, OpCase::LeftConstant>(a, i), undec(b[i]));
                }
            }
        }
        else
            for (size_t i = 0; i < size; ++i)
                c[i] = apply_func(unwrap<op_case, OpCase::LeftConstant>(a, i), unwrap<op_case, OpCase::RightConstant>(b, i));
    }

    static constexpr bool is_plus_minus =   IsOperation<Operation>::plus ||
                                            IsOperation<Operation>::minus;
    static constexpr bool is_multiply =     IsOperation<Operation>::multiply;
    static constexpr bool is_float_division = IsOperation<Operation>::div_floating;
    static constexpr bool is_int_division = IsOperation<Operation>::int_div ||
                                            IsOperation<Operation>::int_div_or_zero;
    static constexpr bool is_division = is_float_division || is_int_division;
    static constexpr bool is_compare =      IsOperation<Operation>::least ||
                                            IsOperation<Operation>::greatest;
    static constexpr bool is_plus_minus_compare = is_plus_minus || is_compare;
    static constexpr bool can_overflow = is_plus_minus || is_multiply;

    using Op = std::conditional_t<is_float_division,
        DivideIntegralImpl<NativeResultType, NativeResultType>, /// substitute divide by intDiv (throw on division by zero)
        Operation<NativeResultType, NativeResultType>>;

    template <OpCase op_case, OpCase target, class E>
    static auto unwrap(const E& elem, size_t i)
    {
        if constexpr (op_case == target)
            return undec(elem);
        else
            return undec(elem[i]);
    }

    /// there's implicit type conversion here
    static NativeResultType apply(NativeResultType a, NativeResultType b)
    {
        if constexpr (can_overflow && check_overflow)
        {
            NativeResultType res;
            if (Op::template apply<NativeResultType>(a, b, res))
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");
            return res;
        }
        else
            return Op::template apply<NativeResultType>(a, b);
    }

    template <bool scale_left, bool may_check_overflow = true>
    static NO_SANITIZE_UNDEFINED NativeResultType applyScaled(NativeResultType a, NativeResultType b, NativeResultType scale)
    {
        static_assert(is_plus_minus_compare || is_multiply);
        NativeResultType res;

        if constexpr (check_overflow && may_check_overflow)
        {
            bool overflow = false;

            if constexpr (scale_left)
                overflow |= common::mulOverflow(a, scale, a);
            else
                overflow |= common::mulOverflow(b, scale, b);

            if constexpr (can_overflow)
                overflow |= Op::template apply<NativeResultType>(a, b, res);
            else
                res = Op::template apply<NativeResultType>(a, b);

            if (overflow)
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");
        }
        else
        {
            if constexpr (scale_left)
                a *= scale;
            else
                b *= scale;
            res = Op::template apply<NativeResultType>(a, b);
        }

        return res;
    }

    template <bool is_decimal_a>
    static NO_SANITIZE_UNDEFINED NativeResultType applyScaledDiv(NativeResultType a, NativeResultType b, NativeResultType scale)
    {
        if constexpr (is_division)
        {
            if constexpr (check_overflow)
            {
                bool overflow = false;
                if constexpr (!is_decimal_a)
                    overflow |= common::mulOverflow(scale, scale, scale);
                overflow |= common::mulOverflow(a, scale, a);
                if (overflow)
                    throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");
            }
            else
            {
                if constexpr (!is_decimal_a)
                    scale *= scale;
                a *= scale;
            }

            return Op::template apply<NativeResultType>(a, b);
        }
    }
};
}

using namespace traits_;
using namespace impl_;

template <template <typename, typename> class Op, typename Name, bool valid_on_default_arguments = true, bool valid_on_float_arguments = true, bool division_by_nullable = false>
class FunctionBinaryArithmetic : public IFunction
{
    static constexpr bool is_plus = IsOperation<Op>::plus;
    static constexpr bool is_minus = IsOperation<Op>::minus;
    static constexpr bool is_multiply = IsOperation<Op>::multiply;
    static constexpr bool is_division = IsOperation<Op>::division;
    static constexpr bool is_bit_hamming_distance = IsOperation<Op>::bit_hamming_distance;
    static constexpr bool is_modulo = IsOperation<Op>::modulo;
    static constexpr bool is_positive_modulo = IsOperation<Op>::positive_modulo;
    static constexpr bool is_int_div = IsOperation<Op>::int_div;
    static constexpr bool is_int_div_or_zero = IsOperation<Op>::int_div_or_zero;
    static constexpr bool is_division_or_null = IsOperation<Op>::division_or_null;

    ContextPtr context;
    bool check_decimal_overflow = true;

    static bool castType(const IDataType * type, auto && f)
    {
        using IntegerTypes = TypeList<
            DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256,
            DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64, DataTypeInt128, DataTypeInt256>;

        using DecimalTypes = TypeList<DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128, DataTypeDecimal256>;

        using Floats = TypeList<DataTypeFloat32, DataTypeFloat64, DataTypeBFloat16>;

        /// Only include extra types that this specific operation actually uses.
        /// Decimal: needed only when allow_decimal is true (plus, minus, multiply,
        ///   divide, intDiv, intDivOrZero, modulo, positiveModulo, least, greatest,
        ///   midpoint). All other operations (bitwise, GCD/LCM, *OrNull division
        ///   variants, etc.) produce InvalidType for Decimal pairs, so skip them.
        /// Date/DateTime/Time: needed for plus, minus, least, greatest, modulo,
        ///   positive_modulo (see BinaryOperationTraits::ResultDataType).
        ///   All other operations produce InvalidType for these, so skip them.
        /// String/FixedString: needed for bitwise ops (allow_fixed_string/allow_string_integer)
        ///   and bitHammingDistance.
        /// Interval: needed for plus/minus (date/time +/- interval arithmetic).
        /// All other operations reject these types in the dispatch lambda anyway,
        /// so we skip them to reduce the template instantiation matrix.
        static constexpr bool needs_decimal = IsOperation<Op>::allow_decimal;
        static constexpr bool needs_date_time = is_plus || is_minus
            || IsOperation<Op>::least || IsOperation<Op>::greatest
            || is_modulo || IsOperation<Op>::positive_modulo;
        static constexpr bool needs_string_types =
            Op<UInt8, UInt8>::allow_fixed_string || Op<UInt8, UInt8>::allow_string_integer || is_bit_hamming_distance;
        static constexpr bool needs_interval = is_plus || is_minus;

        using NumericTypes = std::conditional_t<needs_decimal,
            TypeListConcat<IntegerTypes, DecimalTypes>, IntegerTypes>;
        using NumericAndDateTypes = std::conditional_t<needs_date_time,
            TypeListConcat<NumericTypes, TypeList<DataTypeDate, DataTypeDateTime, DataTypeTime>>, NumericTypes>;
        using WithStrings = std::conditional_t<needs_string_types,
            TypeListConcat<NumericAndDateTypes, TypeList<DataTypeFixedString, DataTypeString>>, NumericAndDateTypes>;
        using WithInterval = std::conditional_t<needs_interval,
            TypeListConcat<WithStrings, TypeList<DataTypeInterval>>, WithStrings>;
        using ValidTypes = std::conditional_t<valid_on_float_arguments,
            TypeListConcat<WithInterval, Floats>, WithInterval>;

        return castTypeToEither(ValidTypes{}, type, std::forward<decltype(f)>(f));
    }

    template <typename F>
    static bool castBothTypes(const IDataType * left, const IDataType * right, F && f)
    {
        return castType(left, [&](const auto & left_)
        {
            return castType(right, [&](const auto & right_)
            {
                return f(left_, right_);
            });
        });
    }

    static FunctionOverloadResolverPtr
    getFunctionForIntervalArithmetic(const DataTypePtr & type0, const DataTypePtr & type1, ContextPtr context)
    {
        bool first_arg_is_date_or_time_or_datetime_or_string = isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(type0) || isString(type0);
        bool second_arg_is_date_or_time_or_datetime_or_string = isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(type1) || isString(type1);

        /// Exactly one argument must be Date or DateTime or String
        if (first_arg_is_date_or_time_or_datetime_or_string == second_arg_is_date_or_time_or_datetime_or_string)
            return {};

        /// Special case when the function is plus or minus, one of arguments is Date or DateTime or String and another is Interval.
        /// We construct another function (example: addMonths) and call it.

        if constexpr (!is_plus && !is_minus)
            return {};

        const DataTypePtr & type_time = first_arg_is_date_or_time_or_datetime_or_string ? type0 : type1;
        const DataTypePtr & type_interval = first_arg_is_date_or_time_or_datetime_or_string ? type1 : type0;

        bool first_or_second_arg_is_string = isString(type0) || isString(type1);
        bool interval_is_number = isNumber(type_interval);

        const DataTypeInterval * interval_data_type = nullptr;
        if (!interval_is_number)
        {
            interval_data_type = checkAndGetDataType<DataTypeInterval>(type_interval.get());

            if (!interval_data_type)
                return {};
        }
        else if (first_or_second_arg_is_string)
        {
            return {};
        }

        if (second_arg_is_date_or_time_or_datetime_or_string && is_minus)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Wrong order of arguments for function {}: "
                                                                  "argument of type Interval cannot be first", name);

        std::string function_name;
        if (interval_data_type)
        {
            function_name = fmt::format("{}{}s",
                is_plus ? "add" : "subtract",
                interval_data_type->getKind().toString());
        }
        else
        {
            if (isDateOrDate32(type_time))
                function_name = is_plus ? "addDays" : "subtractDays";
            else
                function_name = is_plus ? "addSeconds" : "subtractSeconds";
        }

        return FunctionFactory::instance().get(function_name, context);
    }

    static FunctionOverloadResolverPtr
    getFunctionForDateTupleOfIntervalsArithmetic(const DataTypePtr & type0, const DataTypePtr & type1, ContextPtr context)
    {
        bool first_arg_is_date_or_datetime = isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(type0);
        bool second_arg_is_date_or_datetime = isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(type1);

        /// Exactly one argument must be Date or DateTime
        if (first_arg_is_date_or_datetime == second_arg_is_date_or_datetime)
            return {};

        if (!isTuple(type0) && !isTuple(type1))
            return {};

        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Tuple.
        /// We construct another function and call it.
        if constexpr (!is_plus && !is_minus)
            return {};

        if (isTuple(type0) && second_arg_is_date_or_datetime && is_minus)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Wrong order of arguments for function {}: "
                                                                  "argument of Tuple type cannot be first", name);

        std::string function_name;
        if (is_plus)
        {
            function_name = "addTupleOfIntervals";
        }
        else
        {
            function_name = "subtractTupleOfIntervals";
        }

        return FunctionFactory::instance().get(function_name, context);
    }

    static FunctionOverloadResolverPtr
    getFunctionForMergeIntervalsArithmetic(const DataTypePtr & type0, const DataTypePtr & type1, ContextPtr context)
    {
        /// Special case when the function is plus or minus, first argument is Interval or Tuple of Intervals
        ///  and the second argument is the Interval of a different kind.
        /// We construct another function (example: addIntervals) and call it

        if constexpr (!is_plus && !is_minus)
            return {};

        const auto * tuple_data_type_0 = checkAndGetDataType<DataTypeTuple>(type0.get());
        const auto * interval_data_type_0 = checkAndGetDataType<DataTypeInterval>(type0.get());
        const auto * interval_data_type_1 = checkAndGetDataType<DataTypeInterval>(type1.get());

        if ((!tuple_data_type_0 && !interval_data_type_0) || !interval_data_type_1)
            return {};

        if (interval_data_type_0 && interval_data_type_0->equals(*interval_data_type_1))
            return {};

        if (tuple_data_type_0)
        {
            const auto & tuple_types = tuple_data_type_0->getElements();
            for (const auto & type : tuple_types)
                if (!isInterval(type))
                    return {};
        }

        std::string function_name;
        if (is_plus)
        {
            function_name = "addInterval";
        }
        else
        {
            function_name = "subtractInterval";
        }

        return FunctionFactory::instance().get(function_name, context);
    }

    static FunctionOverloadResolverPtr
    getFunctionForTupleArithmetic(const DataTypePtr & type0, const DataTypePtr & type1, ContextPtr context)
    {
        if (!isTuple(removeNullable(type0)) || !isTuple(removeNullable(type1)))
            return {};

        /// Special case when the function is plus, minus or multiply, both arguments are tuples.
        /// We construct another function (example: tuplePlus) and call it.

        if constexpr (!is_plus && !is_minus && !is_multiply)
            return {};

        std::string function_name;
        if (is_plus)
        {
            function_name = "tuplePlus";
        }
        else if (is_minus)
        {
            function_name = "tupleMinus";
        }
        else
        {
            function_name = "dotProduct";
        }

        return FunctionFactory::instance().get(function_name, context);
    }

    static FunctionOverloadResolverPtr
    getFunctionForTupleAndNumberArithmetic(const DataTypePtr & type0, const DataTypePtr & type1, ContextPtr context)
    {
        if (!(isTuple(removeNullable(type0)) && isNumber(removeNullable(type1)))
            && !(isTuple(removeNullable(type1)) && isNumber(removeNullable(type0))))
            return {};

        /// Special case when the function is multiply or divide, one of arguments is Tuple and another is Number.
        /// We construct another function (example: tupleMultiplyByNumber) and call it.

        if constexpr (!is_multiply && !is_division && !is_positive_modulo)
            return {};

        if (isNumber(type0) && (is_division || is_positive_modulo))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Wrong order of arguments for function {}: "
                                                                  "argument of numeric type cannot be first", name);

        std::string function_name;
        if constexpr (is_multiply)
        {
            function_name = "tupleMultiplyByNumber";
        }
        else if constexpr (is_positive_modulo)
        {
            function_name = "tuplePositiveModuloByNumber";
        }
        else // is_division
        {
            if constexpr (is_modulo)
            {
                function_name = "tupleModuloByNumber";
            }
            else if constexpr (is_int_div)
            {
                function_name = "tupleIntDivByNumber";
            }
            else if constexpr (is_int_div_or_zero)
            {
                function_name = "tupleIntDivOrZeroByNumber";
            }
            else
            {
                function_name = "tupleDivideByNumber";
            }
        }

        return FunctionFactory::instance().get(function_name, context);
    }

    static bool isAggregateMultiply(const DataTypePtr & type0, const DataTypePtr & type1)
    {
        if constexpr (!is_multiply)
            return false;

        WhichDataType which0(type0);
        WhichDataType which1(type1);

        return (which0.isAggregateFunction() && which1.isNativeUInt())
            || (which0.isNativeUInt() && which1.isAggregateFunction());
    }

    static bool isAggregateAddition(const DataTypePtr & type0, const DataTypePtr & type1)
    {
        if constexpr (!is_plus)
            return false;

        WhichDataType which0(type0);
        WhichDataType which1(type1);

        return which0.isAggregateFunction() && which1.isAggregateFunction();
    }

    static bool isDateTime64Subtraction(const DataTypePtr & type0, const DataTypePtr & type1)
    {
        if constexpr (!is_minus)
            return false;

        WhichDataType which0(type0);
        WhichDataType which1(type1);

        if (which0.isDateTime64() && which1.isDateTimeOrDateTime64())
            return true;

        return which1.isDateTime64() && which0.isDateTimeOrDateTime64();
    }

    static bool isTime64Subtraction(const DataTypePtr & type0, const DataTypePtr & type1)
    {
        if constexpr (!is_minus)
            return false;

        WhichDataType which0(type0);
        WhichDataType which1(type1);

        if (which0.isTime64() && which1.isTimeOrTime64())
            return true;

        return which1.isTime64() && which0.isTimeOrTime64();
    }

    static bool isDateAndTimeAddition(const DataTypePtr & type0, const DataTypePtr & type1)
    {
        if constexpr (!is_plus)
            return false;

        WhichDataType which0(type0);
        WhichDataType which1(type1);

        return (which0.isDateOrDate32() && which1.isTimeOrTime64())
            || (which0.isTimeOrTime64() && which1.isDateOrDate32());
    }

    /// Multiply aggregation state by integer constant: by merging it with itself specified number of times.
    ColumnPtr executeAggregateMultiply(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        ColumnsWithTypeAndName new_arguments = arguments;
        if (WhichDataType(new_arguments[1].type).isAggregateFunction())
            std::swap(new_arguments[0], new_arguments[1]);

        if (!isColumnConst(*new_arguments[1].column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of aggregation state multiply. "
                "Should be integer constant", new_arguments[1].column->getName());

        const IColumn & agg_state_column = *new_arguments[0].column;
        bool agg_state_is_const = isColumnConst(agg_state_column);
        const ColumnAggregateFunction & column = typeid_cast<const ColumnAggregateFunction &>(
            agg_state_is_const ? assert_cast<const ColumnConst &>(agg_state_column).getDataColumn() : agg_state_column);

        AggregateFunctionPtr function = column.getAggregateFunction();

        size_t size = agg_state_is_const ? 1 : input_rows_count;

        auto column_to = ColumnAggregateFunction::create(function);
        column_to->reserve(size);

        auto column_from = ColumnAggregateFunction::create(function);
        column_from->reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            column_to->insertDefault();
            column_from->insertFrom(column.getData()[i]);
        }

        auto & vec_to = column_to->getData();
        auto & vec_from = column_from->getData();

        UInt64 m = typeid_cast<const ColumnConst *>(new_arguments[1].column.get())->getValue<UInt64>();

        // Since we merge the function states by ourselves, we have to have an
        // Arena for this. Pass it to the resulting column so that the arena
        // has a proper lifetime.
        auto arena = std::make_shared<Arena>();
        column_to->addArena(arena);

        /// We use exponentiation by squaring algorithm to perform multiplying aggregate states by N in O(log(N)) operations
        /// https://en.wikipedia.org/wiki/Exponentiation_by_squaring
        while (m)
        {
            if (m % 2)
            {
                for (size_t i = 0; i < size; ++i)
                    function->merge(vec_to[i], vec_from[i], arena.get());
                --m;
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                    function->merge(vec_from[i], vec_from[i], arena.get());
                m /= 2;
            }
        }

        if (agg_state_is_const)
            return ColumnConst::create(std::move(column_to), input_rows_count);
        return column_to;
    }

    /// Merge two aggregation states together.
    ColumnPtr executeAggregateAddition(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        const IColumn & lhs_column = *arguments[0].column;
        const IColumn & rhs_column = *arguments[1].column;

        bool lhs_is_const = isColumnConst(lhs_column);
        bool rhs_is_const = isColumnConst(rhs_column);

        const ColumnAggregateFunction & lhs = typeid_cast<const ColumnAggregateFunction &>(
            lhs_is_const ? assert_cast<const ColumnConst &>(lhs_column).getDataColumn() : lhs_column);
        const ColumnAggregateFunction & rhs = typeid_cast<const ColumnAggregateFunction &>(
            rhs_is_const ? assert_cast<const ColumnConst &>(rhs_column).getDataColumn() : rhs_column);

        AggregateFunctionPtr function = lhs.getAggregateFunction();

        size_t size = (lhs_is_const && rhs_is_const) ? 1 : input_rows_count;

        auto column_to = ColumnAggregateFunction::create(function);
        column_to->reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            column_to->insertFrom(lhs.getData()[lhs_is_const ? 0 : i]);
            column_to->insertMergeFrom(rhs.getData()[rhs_is_const ? 0 : i]);
        }

        if (lhs_is_const && rhs_is_const)
            return ColumnConst::create(std::move(column_to), input_rows_count);
        return column_to;
    }

    ColumnPtr executeDateTime64Subtraction(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                                           size_t input_row_count) const
    {
        using OpImplCheck = DecimalBinaryOperation<Op, Decimal64, /* check_overflow */ true>;
        using OpImpl = DecimalBinaryOperation<Op, Decimal64, /* check_overflow */ false>;
        using ColumnDateTime64 = DataTypeDateTime64::ColumnType;
        using ColumnDateTime = DataTypeDateTime::ColumnType;

        struct ColumnInfo
        {
            explicit ColumnInfo(const ColumnWithTypeAndName & argument_) : argument(argument_), converted_col(nullptr) {}
            const ColumnWithTypeAndName & argument;
            const ColumnDateTime64 * col;
            ColumnPtr converted_col;
            UInt64 const_val;
            bool is_const;
            UInt64 scale;
        } cols[2]{ColumnInfo{arguments[0]}, ColumnInfo{arguments[1]}};

        const auto * type = checkAndGetDataType<DataTypeDecimal<Decimal64>>(result_type.get());
        if (!type)
            return nullptr;

        /// Process column information for both arguments
        auto process = [type] (struct ColumnInfo & to_be_checked, const DataTypePtr & other_type)
        {
            const auto * col_raw = to_be_checked.argument.column.get();
            to_be_checked.is_const = isColumnConst(*col_raw);

            if (to_be_checked.is_const)
            {
                if (WhichDataType(to_be_checked.argument.type).isDateTime())
                {
                    to_be_checked.const_val = checkAndGetColumnConst<ColumnDateTime>(col_raw)->template getValue<UInt32>();
                    /// the output type is the same as the other argument, which is DateTime64
                    to_be_checked.scale = type->getScaleMultiplier();
                }
                else
                {
                    to_be_checked.const_val = checkAndGetColumnConst<ColumnDateTime64>(col_raw)->template getValue<Decimal64>().value;
                    const auto & from_type = *checkAndGetDataType<DataTypeDateTime64>(to_be_checked.argument.type.get());
                    to_be_checked.scale = type->scaleFactorFor(from_type, /* is_multiply_or_divisor */ false);
                }
            }
            else
            {
                if (WhichDataType(to_be_checked.argument.type).isDateTime())
                {
                    to_be_checked.converted_col = castColumn(to_be_checked.argument, other_type);
                    to_be_checked.col = checkAndGetColumn<ColumnDateTime64>(to_be_checked.converted_col.get());
                    /// the DateTime argument is already scaled up by calling castColumn
                    to_be_checked.scale = 1;
                }
                else
                {
                    to_be_checked.col = checkAndGetColumn<ColumnDateTime64>(col_raw);
                    const auto & from_type = *checkAndGetDataType<DataTypeDateTime64>(to_be_checked.argument.type.get());
                    to_be_checked.scale = type->scaleFactorFor(from_type, /* is_multiply_or_divisor */ false);
                }

                if (!to_be_checked.col)
                    return false;
            }
            return true;
        };

        /// Process both columns
        if (!process(cols[0], cols[1].argument.type))
            return nullptr;
        if (!process(cols[1], cols[0].argument.type))
            return nullptr;

        /// Handle constant values
        if (cols[0].is_const && cols[1].is_const)
        {
            auto res = helperInvokeEither</* left_is_decimal */ true, /* right_is_decimal */ true, OpImpl, OpImplCheck, Decimal64>(
                cols[0].const_val, cols[1].const_val, cols[0].scale, cols[1].scale);
            return type->createColumnConst(input_row_count, toField(res, type->getScale()));
        }

        auto col_res = ColumnDecimal<Decimal64>::create(input_row_count, type->getScale());
        auto & vec_res = col_res->getData();

        auto invoke = [&]<OpCase op_case>(const auto& col0, const auto& col1)
        {
            helperInvokeEither<op_case, /* left_is_decimal */ true, /* right_is_decimal */ true, OpImpl, OpImplCheck>(
                col0, col1, vec_res, cols[0].scale, cols[1].scale, nullptr);
        };
        /// Process based on whether the column is constant or not
        if (cols[0].is_const)
            invoke.template operator()<OpCase::LeftConstant>(cols[0].const_val, cols[1].col->getData());
        else if (cols[1].is_const)
            invoke.template operator()<OpCase::RightConstant>(cols[0].col->getData(), cols[1].const_val);
        else
            invoke.template operator()<OpCase::Vector>(cols[0].col->getData(), cols[1].col->getData());

        return col_res;
    }

    ColumnPtr executeTime64Subtraction(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                                           size_t input_row_count) const
    {
        using OpImplCheck = DecimalBinaryOperation<Op, Decimal64, /* check_overflow */ true>;
        using OpImpl = DecimalBinaryOperation<Op, Decimal64, /* check_overflow */ false>;
        using ColumnTime64 = DataTypeTime64::ColumnType;
        using ColumnTime = DataTypeTime::ColumnType;

        struct ColumnInfo
        {
            explicit ColumnInfo(const ColumnWithTypeAndName & argument_) : argument(argument_), converted_col(nullptr) {}
            const ColumnWithTypeAndName & argument;
            const ColumnTime64 * col;
            ColumnPtr converted_col;
            UInt64 const_val;
            bool is_const;
            UInt64 scale;
        } cols[2]{ColumnInfo{arguments[0]}, ColumnInfo{arguments[1]}};

        const auto * type = checkAndGetDataType<DataTypeDecimal<Decimal64>>(result_type.get());
        if (!type)
            return nullptr;

        /// Process column information for both arguments
        auto process = [type] (struct ColumnInfo & to_be_checked, const DataTypePtr & other_type)
        {
            const auto * col_raw = to_be_checked.argument.column.get();
            to_be_checked.is_const = isColumnConst(*col_raw);

            if (to_be_checked.is_const)
            {
                if (WhichDataType(to_be_checked.argument.type).isTime())
                {
                    to_be_checked.const_val = checkAndGetColumnConst<ColumnTime>(col_raw)->template getValue<UInt32>();
                    /// the output type is the same as the other argument, which is Time64
                    to_be_checked.scale = type->getScaleMultiplier();
                }
                else
                {
                    to_be_checked.const_val = checkAndGetColumnConst<ColumnTime64>(col_raw)->template getValue<Decimal64>().value;
                    const auto & from_type = *checkAndGetDataType<DataTypeTime64>(to_be_checked.argument.type.get());
                    to_be_checked.scale = type->scaleFactorFor(from_type, /* is_multiply_or_divisor */ false);
                }
            }
            else
            {
                if (WhichDataType(to_be_checked.argument.type).isTime())
                {
                    to_be_checked.converted_col = castColumn(to_be_checked.argument, other_type);
                    to_be_checked.col = checkAndGetColumn<ColumnTime64>(to_be_checked.converted_col.get());
                    /// the DateTime argument is already scaled up by calling castColumn
                    to_be_checked.scale = 1;
                }
                else
                {
                    to_be_checked.col = checkAndGetColumn<ColumnTime64>(col_raw);
                    const auto & from_type = *checkAndGetDataType<DataTypeTime64>(to_be_checked.argument.type.get());
                    to_be_checked.scale = type->scaleFactorFor(from_type, /* is_multiply_or_divisor */ false);
                }

                if (!to_be_checked.col)
                    return false;
            }
            return true;
        };

        /// Process both columns
        if (!process(cols[0], cols[1].argument.type))
            return nullptr;
        if (!process(cols[1], cols[0].argument.type))
            return nullptr;

        /// Handle constant values
        if (cols[0].is_const && cols[1].is_const)
        {
            auto res = helperInvokeEither</* left_is_decimal */ true, /* right_is_decimal */ true, OpImpl, OpImplCheck, Decimal64>(
                cols[0].const_val, cols[1].const_val, cols[0].scale, cols[1].scale);
            return type->createColumnConst(input_row_count, toField(res, type->getScale()));
        }

        auto col_res = ColumnDecimal<Decimal64>::create(input_row_count, type->getScale());
        auto & vec_res = col_res->getData();

        auto invoke = [&]<OpCase op_case>(const auto& col0, const auto& col1)
        {
            helperInvokeEither<op_case, /* left_is_decimal */ true, /* right_is_decimal */ true, OpImpl, OpImplCheck>(
                col0, col1, vec_res, cols[0].scale, cols[1].scale, nullptr);
        };
        /// Process based on whether the column is constant or not
        if (cols[0].is_const)
            invoke.template operator()<OpCase::LeftConstant>(cols[0].const_val, cols[1].col->getData());
        else if (cols[1].is_const)
            invoke.template operator()<OpCase::RightConstant>(cols[0].col->getData(), cols[1].const_val);
        else
            invoke.template operator()<OpCase::Vector>(cols[0].col->getData(), cols[1].col->getData());

        return col_res;
    }

    /// Executes Date/Date32 + Time/Time64 -> DateTime/DateTime64.
    ///
    /// Converts the date to a midnight unix timestamp, then adds the time-of-day offset.
    /// For Time64, the midnight timestamp is scaled to match Time64's precision (e.g. milliseconds)
    /// before adding. The arithmetic uses Int128 so that the scaling cannot overflow.
    ///
    /// Result type: Date + Time -> DateTime, all other combinations -> DateTime64(scale).
    /// Overflow: respects date_time_overflow_behavior (throw / saturate / ignore).
    ColumnPtr executeDateAndTimeAddition(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        /// The operation is commutative — figure out which argument is the date and which is the time.
        WhichDataType which0(arguments[0].type);
        size_t date_idx = which0.isDateOrDate32() ? 0 : 1;
        const auto & date_arg = arguments[date_idx];
        const auto & time_arg = arguments[1 - date_idx];

        bool is_date32 = WhichDataType(date_arg.type).isDate32();
        bool is_time64 = WhichDataType(time_arg.type).isTime64();

        /// Date + Time -> DateTime (UInt32 seconds). All other combinations -> DateTime64
        /// because either Date32's range exceeds UInt32, or Time64 requires sub-second precision.
        bool result_is_datetime64 = is_date32 || is_time64;

        /// Session timezone if set, otherwise server default. Used by fromDayNum to convert
        /// a day number to a midnight unix timestamp — different timezones give different timestamps.
        const auto & time_zone = DateLUT::instance();

        /// Time64 values are stored in scaled units (e.g. milliseconds at scale 3, nanoseconds at scale 9).
        /// The midnight timestamp is in seconds, so we multiply it by this factor to match Time64's units.
        /// For plain Time (seconds), scale_multiplier stays 1.
        Int64 scale_multiplier = 1;
        if (is_time64)
        {
            const auto * time64_type = checkAndGetDataType<DataTypeTime64>(time_arg.type.get());
            scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(time64_type->getScale());
        }

        auto overflow_behavior = getDateTimeOverflowBehavior(context);

        /// The valid range for the result, expressed in the result's own units:
        ///   DateTime:   seconds in [0, 2^32-1], covering ~1970 to ~2106.
        ///   DateTime64: scaled values in [MIN_DATETIME64_TIMESTAMP * 10^scale, MAX_DATETIME64_TIMESTAMP * 10^scale],
        ///               covering ~1900 to ~2299 (but narrower at scale 9 due to Int64 capacity).
        Int64 result_min = 0;
        Int64 result_max = static_cast<Int64>(MAX_DATETIME_TIMESTAMP);
        UInt32 result_scale = 0;
        if (result_is_datetime64)
        {
            const auto & res_type = assert_cast<const DataTypeDateTime64 &>(*result_type);
            result_scale = res_type.getScale();
            Int64 result_scale_mul = DecimalUtils::scaleMultiplier<Int64>(result_scale);
            /// The min side (1900-01-01) fits in Int64 for all supported scales (0-9).
            /// The max side (2299-12-31) overflows Int64 at scale 9; clamp to Int64 max in that case.
            result_min = MIN_DATETIME64_TIMESTAMP * result_scale_mul;
            result_max = (MAX_DATETIME64_TIMESTAMP <= std::numeric_limits<Int64>::max() / result_scale_mul)
                ? MAX_DATETIME64_TIMESTAMP * result_scale_mul + result_scale_mul - 1
                : std::numeric_limits<Int64>::max();
        }

        /// Convert a day number to the unix timestamp (seconds) at midnight of that day.
        auto to_midnight = [&](Int64 day_num) -> Int64
        {
            return is_date32 ? static_cast<Int64>(time_zone.fromDayNum(ExtendedDayNum(static_cast<Int32>(day_num))))
                             : static_cast<Int64>(time_zone.fromDayNum(DayNum(static_cast<UInt16>(day_num))));
        };

        /// Check whether the computed result fits in the valid range.
        /// The result comes in as Int128 (which cannot overflow), and is narrowed to Int64 here.
        auto check_and_clamp = [&](Int128 wide_result) -> Int64
        {
            if (wide_result >= result_min && wide_result <= result_max) [[likely]]
                return static_cast<Int64>(wide_result);

            if (overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
            {
                if (result_is_datetime64)
                    throw Exception(
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                        "The result of Date plus Time is out of bounds of type DateTime64({})",
                        result_scale);
                else
                    throw Exception(
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                        "Value {} is out of bounds of type DateTime",
                        static_cast<Int64>(wide_result));
            }
            else if (overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Saturate)
            {
                return static_cast<Int64>(std::clamp<Int128>(wide_result, result_min, result_max));
            }
            /// Ignore: truncate to Int64. The result is undefined per ClickHouse docs.
            return static_cast<Int64>(wide_result);
        };

        /// Full computation: look up midnight from the day number, scale it, add time, check bounds.
        auto compute_from_day = [&](Int64 day_num, Int64 time_val) -> Int64
        {
            Int64 midnight_ts = to_midnight(day_num);

            if (is_time64)
                return check_and_clamp(Int128(midnight_ts) * scale_multiplier + time_val);

            return check_and_clamp(Int128(midnight_ts) + time_val);
        };

        /// When the date is a constant, midnight (already scaled) is precomputed before the loop.
        /// This skips the day-number lookup and the multiply on every row.
        auto compute_from_precomputed_midnight
            = [&](Int128 midnight_wide, Int64 time_val) -> Int64 { return check_and_clamp(midnight_wide + time_val); };

        /// Resolve column data to typed raw pointers once, so the per-row loop
        /// only does pointer indexing — no dynamic_cast or type checks per row.
        const auto * date_col = date_arg.column.get();
        const auto * time_col = time_arg.column.get();
        bool date_is_const = isColumnConst(*date_col);
        bool time_is_const = isColumnConst(*time_col);

        Int64 date_const_value = 0;
        const Int32 * date32_ptr = nullptr;
        const UInt16 * date16_ptr = nullptr;
        if (date_is_const)
            date_const_value = is_date32 ? checkAndGetColumnConst<ColumnVector<Int32>>(date_col)->template getValue<Int32>()
                                         : checkAndGetColumnConst<ColumnVector<UInt16>>(date_col)->template getValue<UInt16>();
        else if (is_date32)
            date32_ptr = checkAndGetColumn<ColumnVector<Int32>>(date_col)->getData().data();
        else
            date16_ptr = checkAndGetColumn<ColumnVector<UInt16>>(date_col)->getData().data();

        Int64 time_const_value = 0;
        const Time64 * time64_ptr = nullptr;
        const Int32 * time32_ptr = nullptr;
        if (time_is_const)
            time_const_value = is_time64 ? checkAndGetColumnConst<ColumnDecimal<Time64>>(time_col)->template getValue<Time64>().value
                                         : checkAndGetColumnConst<ColumnVector<Int32>>(time_col)->template getValue<Int32>();
        else if (is_time64)
            time64_ptr = checkAndGetColumn<ColumnDecimal<Time64>>(time_col)->getData().data();
        else
            time32_ptr = checkAndGetColumn<ColumnVector<Int32>>(time_col)->getData().data();

        auto get_date = [&](size_t i) -> Int64
        {
            if (date_is_const)
                return date_const_value;
            return is_date32 ? static_cast<Int64>(date32_ptr[i]) : static_cast<Int64>(date16_ptr[i]);
        };
        auto get_time = [&](size_t i) -> Int64
        {
            if (time_is_const)
                return time_const_value;
            return is_time64 ? time64_ptr[i].value : static_cast<Int64>(time32_ptr[i]);
        };

        /// When the date column is constant, compute midnight once (as Int128, already scaled
        /// for Time64) so the loop only needs to add each row's time value.
        Int128 midnight_precomputed = 0;
        if (date_is_const)
        {
            Int64 midnight_ts = to_midnight(date_const_value);
            midnight_precomputed = is_time64 ? Int128(midnight_ts) * scale_multiplier : Int128(midnight_ts);
        }

        /// Both columns constant — compute once, return a single-value column.
        if (date_is_const && time_is_const)
        {
            Int64 const_result = compute_from_precomputed_midnight(midnight_precomputed, time_const_value);
            if (result_is_datetime64)
                return result_type->createColumnConst(input_rows_count, DecimalField<DateTime64>(DateTime64(const_result), result_scale));
            return result_type->createColumnConst(input_rows_count, static_cast<UInt64>(static_cast<UInt32>(const_result)));
        }

        if (result_is_datetime64)
        {
            auto col_res = ColumnDecimal<DateTime64>::create(input_rows_count, result_scale);
            auto & result_data = col_res->getData();
            if (date_is_const)
                for (size_t i = 0; i < input_rows_count; ++i)
                    result_data[i] = DateTime64(compute_from_precomputed_midnight(midnight_precomputed, get_time(i)));
            else
                for (size_t i = 0; i < input_rows_count; ++i)
                    result_data[i] = DateTime64(compute_from_day(get_date(i), get_time(i)));
            return col_res;
        }
        else
        {
            auto col_res = ColumnVector<UInt32>::create(input_rows_count);
            auto & result_data = col_res->getData();
            if (date_is_const)
                for (size_t i = 0; i < input_rows_count; ++i)
                    result_data[i] = static_cast<UInt32>(compute_from_precomputed_midnight(midnight_precomputed, get_time(i)));
            else
                for (size_t i = 0; i < input_rows_count; ++i)
                    result_data[i] = static_cast<UInt32>(compute_from_day(get_date(i), get_time(i)));
            return col_res;
        }
    }

    ColumnPtr executeDateTimeIntervalPlusMinus(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                                               size_t input_rows_count, const FunctionOverloadResolverPtr & function_builder) const
    {
        ColumnsWithTypeAndName new_arguments = arguments;

        /// Interval argument must be second.
        if (isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(arguments[1].type) || isString(arguments[1].type))
            std::swap(new_arguments[0], new_arguments[1]);

        /// Change interval argument type to its representation
        if (WhichDataType(new_arguments[1].type).isInterval())
            new_arguments[1].type = std::make_shared<DataTypeNumber<DataTypeInterval::FieldType>>();

        auto function = function_builder->build(new_arguments);
        return function->execute(new_arguments, result_type, input_rows_count, /* dry_run = */ false);
    }

    ColumnPtr executeDateTimeTupleOfIntervalsPlusMinus(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                                               size_t input_rows_count, const FunctionOverloadResolverPtr & function_builder) const
    {
        ColumnsWithTypeAndName new_arguments = arguments;

       /// Tuple argument must be second.
        if (isTuple(arguments[0].type))
            std::swap(new_arguments[0], new_arguments[1]);

        auto function = function_builder->build(new_arguments);

        return function->execute(new_arguments, result_type, input_rows_count, /* dry_run = */ false);
    }

    ColumnPtr executeIntervalTupleOfIntervalsPlusMinus(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                                               size_t input_rows_count, const FunctionOverloadResolverPtr & function_builder) const
    {
        auto function = function_builder->build(arguments);

        return function->execute(arguments, result_type, input_rows_count, /* dry_run = */ false);
    }

    ColumnPtr executeArraysImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        const auto * return_type_array = checkAndGetDataType<DataTypeArray>(result_type.get());

        if (!return_type_array)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Return type for function {} must be array", getName());

        auto num_args = arguments.size();
        DataTypes data_types;

        ColumnsWithTypeAndName new_arguments {num_args};
        DataTypePtr result_array_type;

        const auto * left_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        const auto * right_const = typeid_cast<const ColumnConst *>(arguments[1].column.get());

        /// Unpacking arrays if both are constants.
        if (left_const && right_const)
        {
            new_arguments[0] = {left_const->getDataColumnPtr(), arguments[0].type, arguments[0].name};
            new_arguments[1] = {right_const->getDataColumnPtr(), arguments[1].type, arguments[1].name};
            auto col = executeImpl(new_arguments, result_type, 1);
            return ColumnConst::create(std::move(col), input_rows_count);
        }

        /// Unpacking arrays if at least one column is constant.
        if (left_const || right_const)
        {
            new_arguments[0] = {arguments[0].column->convertToFullColumnIfConst(), arguments[0].type, arguments[0].name};
            new_arguments[1] = {arguments[1].column->convertToFullColumnIfConst(), arguments[1].type, arguments[1].name};
            return executeImpl(new_arguments, result_type, input_rows_count);
        }

        const auto * left_array_col = typeid_cast<const ColumnArray *>(arguments[0].column.get());
        const auto * right_array_col = typeid_cast<const ColumnArray *>(arguments[1].column.get());
        if (!left_array_col->hasEqualOffsets(*right_array_col))
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Two arguments for function {} must have equal sizes", getName());

        const auto & left_array_type = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();
        new_arguments[0] = {left_array_col->getDataPtr(), left_array_type, arguments[0].name};

        const auto & right_array_type = typeid_cast<const DataTypeArray *>(arguments[1].type.get())->getNestedType();
        new_arguments[1] = {right_array_col->getDataPtr(), right_array_type, arguments[1].name};

        result_array_type = typeid_cast<const DataTypeArray *>(result_type.get())->getNestedType();

        size_t rows_count = 0;
        const auto & left_offsets = left_array_col->getOffsets();
        if (!left_offsets.empty())
            rows_count = left_offsets.back();
        auto res = executeImpl(new_arguments, result_array_type, rows_count);

        return ColumnArray::create(res, typeid_cast<const ColumnArray *>(arguments[0].column.get())->getOffsetsPtr());
    }

    ColumnPtr executeArrayWithNumericImpl(const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        ColumnsWithTypeAndName arguments = args;
        bool is_swapped = isNumber(args[0].type); /// Defines the order of arguments (If array is first argument - is_swapped = false)

        const auto * return_type_array = checkAndGetDataType<DataTypeArray>(result_type.get());
        if (!return_type_array)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Return type for function {} must be array", getName());

        auto num_args = arguments.size();
        DataTypes data_types;

        ColumnsWithTypeAndName new_arguments {num_args};
        DataTypePtr result_array_type;

        const auto * left_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        const auto * right_const = typeid_cast<const ColumnConst *>(arguments[1].column.get());

        if (left_const && right_const)
        {
            new_arguments[0] = {left_const->getDataColumnPtr(), arguments[0].type, arguments[0].name};
            new_arguments[1] = {right_const->getDataColumnPtr(), arguments[1].type, arguments[1].name};
            auto col = executeImpl(new_arguments, result_type, 1);
            return ColumnConst::create(std::move(col), input_rows_count);
        }

        if (right_const && is_swapped)
        {
            new_arguments[0] = {arguments[0].column.get()->getPtr(), arguments[0].type, arguments[0].name};
            new_arguments[1] = {right_const->convertToFullColumnIfConst(), arguments[1].type, arguments[1].name};
            return executeImpl(new_arguments, result_type, input_rows_count);
        }
        if (left_const && !is_swapped)
        {
            new_arguments[0] = {left_const->convertToFullColumnIfConst(), arguments[0].type, arguments[0].name};
            new_arguments[1] = {arguments[1].column.get()->getPtr(), arguments[1].type, arguments[1].name};
            return executeImpl(new_arguments, result_type, input_rows_count);
        }

        if (is_swapped)
            std::swap(arguments[1], arguments[0]);

        const auto * left_array_col = typeid_cast<const ColumnArray *>(arguments[0].column.get());
        const auto & left_array_elements_type = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();
        const auto & right_col = arguments[1].column.get()->cloneResized(left_array_col->size());

        size_t rows_count = 0;
        const auto & left_offsets = left_array_col->getOffsets();
        if (!left_offsets.empty())
            rows_count = left_offsets.back();

        new_arguments[0] = {left_array_col->getDataPtr(), left_array_elements_type, arguments[0].name};
        if (right_const)
            new_arguments[1] = {right_col->cloneResized(rows_count), arguments[1].type, arguments[1].name};
        else
            new_arguments[1] = {right_col->replicate(left_array_col->getOffsets()), arguments[1].type, arguments[1].name};

        result_array_type = return_type_array->getNestedType();

        if (is_swapped)
            std::swap(new_arguments[1], new_arguments[0]);
        auto res = executeImpl(new_arguments, result_array_type, rows_count);

        return ColumnArray::create(res, left_array_col->getOffsetsPtr());
    }

    ColumnPtr executeTupleNumberOperator(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                                               size_t input_rows_count, const FunctionOverloadResolverPtr & function_builder) const
    {
        ColumnsWithTypeAndName new_arguments = arguments;

        /// Number argument must be second.
        if (isNumber(arguments[0].type))
            std::swap(new_arguments[0], new_arguments[1]);

        auto function = function_builder->build(new_arguments);

        return function->execute(new_arguments, result_type, input_rows_count, /* dry_run = */ false);
    }

    template <typename T, typename ResultDataType>
    static auto helperGetOrConvert(const auto & col_const, const auto & col)
    {
        using ResultType = typename ResultDataType::FieldType;
        using NativeResultType = NativeType<ResultType>;

        if constexpr (IsFloatingPoint<ResultDataType> && is_decimal<T>)
            return DecimalUtils::convertTo<NativeResultType>(col_const->template getValue<T>(), col.getScale());
        else if constexpr (is_decimal<T>)
            return col_const->template getValue<T>().value;
        else
            return col_const->template getValue<T>();
    }

    template <OpCase op_case, bool left_decimal, bool right_decimal, typename OpImpl, typename OpImplCheck>
    void helperInvokeEither(const auto& left, const auto& right, auto& vec_res, auto scale_a, auto scale_b, const NullMap * right_nullmap) const
    {
        if (check_decimal_overflow)
            OpImplCheck::template process<op_case, left_decimal, right_decimal>(left, right, vec_res, scale_a, scale_b, right_nullmap);
        else
            OpImpl::template process<op_case, left_decimal, right_decimal>(left, right, vec_res, scale_a, scale_b, right_nullmap);
    }

    template <bool left_decimal, bool right_decimal, typename OpImpl, typename OpImplCheck, typename ResultType, typename A, typename B>
    ResultType helperInvokeEither(A a, B b, auto scale_a, auto scale_b) const
        requires(!is_decimal<A> && !is_decimal<B>)
    {
        if (check_decimal_overflow)
            return OpImplCheck::template process<left_decimal, right_decimal>(a, b, scale_a, scale_b);
        else
            return OpImpl::template process<left_decimal, right_decimal>(a, b, scale_a, scale_b);
    }

    template <class LeftDataType, class RightDataType, class ResultDataType>
    ColumnPtr executeNumericWithDecimal(
        const auto & left, const auto & right,
        const ColumnConst * const col_left_const, const ColumnConst * const col_right_const,
        const auto * const col_left, const auto * const col_right,
        size_t col_left_size, const NullMap * right_nullmap) const
    {
        using T0 = typename LeftDataType::FieldType;
        using T1 = typename RightDataType::FieldType;
        using ResultType = typename ResultDataType::FieldType;

        using NativeResultType = NativeType<ResultType>;
        using OpImpl = DecimalBinaryOperation<Op, ResultType, false>;
        using OpImplCheck = DecimalBinaryOperation<Op, ResultType, true>;

        using ColVecResult = ColumnVectorOrDecimal<ResultType>;

        static constexpr const bool left_is_decimal = is_decimal<T0>;
        static constexpr const bool right_is_decimal = is_decimal<T1>;

        typename ColVecResult::MutablePtr col_res = nullptr;

        const ResultDataType type = decimalResultType<is_multiply, is_division>(left, right);

        const ResultType scale_a = [&]
        {
            if constexpr (IsDataTypeDecimal<RightDataType> && is_division)
                return right.getScaleMultiplier(); // the division impl uses only the scale_a
            else
            {
                if constexpr (is_multiply)
                    // the decimal impl uses scales, but if the result is decimal, both of the arguments are decimal,
                    // so they would multiply correctly, so we need to scale the result to the neutral element (1).
                    // The explicit type is needed as the int (in contrast with float) can't be implicitly converted
                    // to decimal.
                    return ResultType{1};
                else
                    return type.scaleFactorFor(left, false);
            }
        }();

        const ResultType scale_b = [&]
        {
                if constexpr (is_multiply)
                    return ResultType{1};
                else
                    return type.scaleFactorFor(right, is_division);
        }();

        /// non-vector result
        if (col_left_const && col_right_const)
        {
            const NativeResultType const_a = static_cast<NativeResultType>(
                helperGetOrConvert<T0, ResultDataType>(col_left_const, left));
            const NativeResultType const_b = static_cast<NativeResultType>(
                helperGetOrConvert<T1, ResultDataType>(col_right_const, right));

            ResultType res = {};
            if (!right_nullmap || !(*right_nullmap)[0])
            {
                res = helperInvokeEither<left_is_decimal, right_is_decimal, OpImpl, OpImplCheck, ResultType>(
                     const_a, const_b, scale_a, scale_b);
            }

            return ResultDataType(type.getPrecision(), type.getScale())
                .createColumnConst(col_left_const->size(), toField(res, type.getScale()));
        }

        col_res = ColVecResult::create(0, type.getScale());

        auto & vec_res = col_res->getData();
        vec_res.resize(col_left_size);

        if (col_left && col_right)
        {
            helperInvokeEither<OpCase::Vector, left_is_decimal, right_is_decimal, OpImpl, OpImplCheck>(
                col_left->getData(), col_right->getData(), vec_res, scale_a, scale_b, right_nullmap);
        }
        else if (col_left_const && col_right)
        {
            const NativeResultType const_a = static_cast<NativeResultType>(
                helperGetOrConvert<T0, ResultDataType>(col_left_const, left));

            helperInvokeEither<OpCase::LeftConstant, left_is_decimal, right_is_decimal, OpImpl, OpImplCheck>(
                const_a, col_right->getData(), vec_res, scale_a, scale_b, right_nullmap);
        }
        else if (col_left && col_right_const)
        {
            const NativeResultType const_b = static_cast<NativeResultType>(
                helperGetOrConvert<T1, ResultDataType>(col_right_const, right));

            helperInvokeEither<OpCase::RightConstant, left_is_decimal, right_is_decimal, OpImpl, OpImplCheck>(
                col_left->getData(), const_b, vec_res, scale_a, scale_b, right_nullmap);
        }
        else
            return nullptr;

        return col_res;
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionBinaryArithmetic>(context); }

    explicit FunctionBinaryArithmetic(ContextPtr context_)
    :   context(context_),
        check_decimal_overflow(decimalCheckArithmeticOverflow(context))
    {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForNulls() const override
    {
        /// We shouldn't use default implementation for nulls for the case when operation is divide,
        /// intDiv or modulo and denominator is Nullable(Something), because it may cause division
        /// by zero error (when value is Null we store default value 0 in nested column).
        /// And we also shouldn't use default implementation for nulls for the case when operation is
        /// divideOrNull, intDivOrNull, moduloOrNull or positiveModuloOrNull, because it will return
        /// null when the divisor is zero, and it is conflict with the default implementation.
        return !division_by_nullable && !is_division_or_null;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return ((IsOperation<Op>::int_div || IsOperation<Op>::modulo || IsOperation<Op>::positive_modulo) && !arguments[1].is_const)
            || (IsOperation<Op>::div_floating
                && (isDecimalOrNullableDecimal(arguments[0].type) || isDecimalOrNullableDecimal(arguments[1].type)));
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return getReturnTypeImplStatic(arguments, context);
    }

    static DataTypePtr getReturnTypeImplStatic(const DataTypes & arguments, ContextPtr context)
    {
        /// Special case when multiply aggregate function state
        if (isAggregateMultiply(arguments[0], arguments[1]))
        {
            if (WhichDataType(arguments[0]).isAggregateFunction())
                return arguments[0];
            return arguments[1];
        }

        /// Special case - addition of two aggregate functions states
        if (isAggregateAddition(arguments[0], arguments[1]))
        {
            if (!arguments[0]->equals(*arguments[1]))
                throw Exception(ErrorCodes::CANNOT_ADD_DIFFERENT_AGGREGATE_STATES,
                    "Cannot add aggregate states of different functions: {} and {}",
                    arguments[0]->getName(), arguments[1]->getName());

            return arguments[0];
        }

        /// Special case - one argument is IPv4 and the other is IPv4 or an integer
        if ((isIPv4(arguments[0]) && (isIPv4(arguments[1]) || isInteger(arguments[1])))
            || (isIPv4(arguments[1]) && isInteger(arguments[0])))
        {
            DataTypes new_arguments {
                    isIPv4(arguments[0]) ? std::make_shared<DataTypeUInt32>() : arguments[0],
                    isIPv4(arguments[1]) ? std::make_shared<DataTypeUInt32>() : arguments[1],
            };

            return getReturnTypeImplStatic2(new_arguments, context);
        }

        /// Special case -one argument is IPv6 and the other is Ipv4 or an integer
        if ((isIPv6(arguments[0]) && (isIPv6(arguments[1]) || isInteger(arguments[1])))
            || (isIPv6(arguments[1]) && isInteger(arguments[0])))
        {
            DataTypes new_arguments {
                    isIPv6(arguments[0]) ? std::make_shared<DataTypeUInt128>() : arguments[0],
                    isIPv6(arguments[1]) ? std::make_shared<DataTypeUInt128>() : arguments[1],
            };

            return getReturnTypeImplStatic2(new_arguments, context);
        }


        if constexpr (is_plus || is_minus)
        {
            if (isArray(arguments[0]) && isArray(arguments[1]))
            {
                DataTypes new_arguments {
                        static_cast<const DataTypeArray &>(*arguments[0]).getNestedType(),
                        static_cast<const DataTypeArray &>(*arguments[1]).getNestedType(),
                };
                return std::make_shared<DataTypeArray>(getReturnTypeImplStatic(new_arguments, context));
            }
        }

        /// Special case when the function is minus, both arguments are DateTime64.
        if (isDateTime64Subtraction(arguments[0], arguments[1]))
        {
            UInt32 scale_lhs = 0;
            UInt32 scale_rhs = 0;
            if (isDateTime64(arguments[0]))
            {
                const auto *lhs = checkAndGetDataType<DataTypeDateTime64>(arguments[0].get());
                scale_lhs = lhs->getScale();
            }
            if (isDateTime64(arguments[1]))
            {
                const auto *rhs = checkAndGetDataType<DataTypeDateTime64>(arguments[1].get());
                scale_rhs = rhs->getScale();
            }
            return std::make_shared<DataTypeDecimal64>(DecimalUtils::max_precision<DateTime64>, std::max(scale_lhs, scale_rhs));
        }
        else if (isTime64Subtraction(arguments[0], arguments[1])) /// Special case when the function is minus, both arguments are Time64.
        {
            UInt32 scale_lhs = 0;
            UInt32 scale_rhs = 0;
            if (isTime64(arguments[0]))
            {
                const auto *lhs = checkAndGetDataType<DataTypeTime64>(arguments[0].get());
                scale_lhs = lhs->getScale();
            }
            if (isTime64(arguments[1]))
            {
                const auto *rhs = checkAndGetDataType<DataTypeTime64>(arguments[1].get());
                scale_rhs = rhs->getScale();
            }
            return std::make_shared<DataTypeDecimal64>(DecimalUtils::max_precision<Time64>, std::max(scale_lhs, scale_rhs));
        }
        else if (isDateAndTimeAddition(arguments[0], arguments[1])) /// Special case when the function is plus, one argument is Date/Date32 and the other is Time/Time64.
        {
            WhichDataType which0(arguments[0]);
            WhichDataType which1(arguments[1]);

            const WhichDataType & date_which = which0.isDateOrDate32() ? which0 : which1;
            const WhichDataType & time_which = which0.isTimeOrTime64() ? which0 : which1;
            const DataTypePtr & time_type = which0.isTimeOrTime64() ? arguments[0] : arguments[1];

            if (date_which.isDate() && time_which.isTime())
            {
                return std::make_shared<DataTypeDateTime>();
            }
            else if (date_which.isDate() && time_which.isTime64())
            {
                const auto * t64 = checkAndGetDataType<DataTypeTime64>(time_type.get());
                return std::make_shared<DataTypeDateTime64>(t64->getScale());
            }
            else if (date_which.isDate32() && time_which.isTime())
            {
                return std::make_shared<DataTypeDateTime64>(0);
            }
            else if (date_which.isDate32() && time_which.isTime64())
            {
                const auto * t64 = checkAndGetDataType<DataTypeTime64>(time_type.get());
                return std::make_shared<DataTypeDateTime64>(t64->getScale());
            }
            else
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unexpected combination of date and time types for plus: {} and {}",
                    arguments[0]->getName(),
                    arguments[1]->getName());
            }
        }

        if constexpr (is_multiply || is_division)
        {
            if (isArray(arguments[0]) && isNumber(arguments[1]))
            {
                DataTypes new_arguments {
                        static_cast<const DataTypeArray &>(*arguments[0]).getNestedType(),
                        arguments[1],
                };
                return std::make_shared<DataTypeArray>(getReturnTypeImplStatic(new_arguments, context));
            }
            if (isNumber(arguments[0]) && isArray(arguments[1]))
            {
                DataTypes new_arguments {
                        arguments[0],
                        static_cast<const DataTypeArray &>(*arguments[1]).getNestedType(),
                };
                return std::make_shared<DataTypeArray>(getReturnTypeImplStatic(new_arguments, context));
            }
        }

        return getReturnTypeImplStatic2(arguments, context);
    }

    static DataTypePtr getReturnTypeImplStatic2(const DataTypes & arguments, ContextPtr context)
    {
        /// Special case when the function is plus or minus, one of arguments is Date/DateTime/String and another is Interval.
        if (auto function_builder = getFunctionForIntervalArithmetic(arguments[0], arguments[1], context))
        {
            ColumnsWithTypeAndName new_arguments(2);

            for (size_t i = 0; i < 2; ++i)
                new_arguments[i].type = arguments[i];

            /// Interval argument must be second.
            if (isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(new_arguments[1].type) || isString(new_arguments[1].type))
                std::swap(new_arguments[0], new_arguments[1]);

            /// Change interval argument to its representation
            new_arguments[1].type = std::make_shared<DataTypeNumber<DataTypeInterval::FieldType>>();

            auto function = function_builder->build(new_arguments);
            return function->getResultType();
        }

        /// Special case when the function is plus, minus or multiply, both arguments are tuples.
        if (auto function_builder = getFunctionForTupleArithmetic(arguments[0], arguments[1], context))
        {
            ColumnsWithTypeAndName new_arguments(2);

            for (size_t i = 0; i < 2; ++i)
                new_arguments[i].type = arguments[i];

            auto function = function_builder->build(new_arguments);
            return function->getResultType();
        }

        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Tuple.
        if (auto function_builder = getFunctionForDateTupleOfIntervalsArithmetic(arguments[0], arguments[1], context))
        {
            ColumnsWithTypeAndName new_arguments(2);

            for (size_t i = 0; i < 2; ++i)
                new_arguments[i].type = arguments[i];

            /// Tuple argument must be second.
            if (isTuple(new_arguments[0].type))
                std::swap(new_arguments[0], new_arguments[1]);

            auto function = function_builder->build(new_arguments);
            return function->getResultType();
        }

        /// Special case when the function is plus or minus, one of arguments is Interval/Tuple of Intervals and another is Interval.
        if (auto function_builder = getFunctionForMergeIntervalsArithmetic(arguments[0], arguments[1], context))
        {
            ColumnsWithTypeAndName new_arguments(2);

            for (size_t i = 0; i < 2; ++i)
                new_arguments[i].type = arguments[i];

            auto function = function_builder->build(new_arguments);
            return function->getResultType();
        }

        /// Special case when the function is multiply or divide, one of arguments is Tuple and another is Number.
        if (auto function_builder = getFunctionForTupleAndNumberArithmetic(arguments[0], arguments[1], context))
        {
            ColumnsWithTypeAndName new_arguments(2);

            for (size_t i = 0; i < 2; ++i)
                new_arguments[i].type = arguments[i];

            /// Number argument must be second.
            if (isNumber(new_arguments[0].type))
                std::swap(new_arguments[0], new_arguments[1]);

            auto function = function_builder->build(new_arguments);
            return function->getResultType();
        }

        DataTypePtr type_res;

        const bool valid = castBothTypes(arguments[0].get(), arguments[1].get(), [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;

            if constexpr ((std::is_same_v<DataTypeFixedString, LeftDataType> || std::is_same_v<DataTypeString, LeftDataType>) ||
                (std::is_same_v<DataTypeFixedString, RightDataType> || std::is_same_v<DataTypeString, RightDataType>))
            {
                if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType> &&
                              std::is_same_v<DataTypeFixedString, RightDataType>)
                {
                    if constexpr (!Op<DataTypeFixedString, DataTypeFixedString>::allow_fixed_string)
                        return false;
                    else
                    {
                        if (left.getN() == right.getN())
                        {
                            if constexpr (is_bit_hamming_distance)
                                type_res = std::make_shared<DataTypeUInt16>();
                            else
                                type_res = std::make_shared<LeftDataType>(left.getN());
                            return true;
                        }
                    }
                }

                if constexpr (
                    is_bit_hamming_distance
                    && std::is_same_v<DataTypeString, LeftDataType> && std::is_same_v<DataTypeString, RightDataType>)
                    type_res = std::make_shared<DataTypeUInt64>();
                else if constexpr (!Op<LeftDataType, RightDataType>::allow_string_integer)
                    return false;
                else if constexpr (!IsIntegral<RightDataType>)
                    return false;
                else if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType>)
                    type_res = std::make_shared<LeftDataType>(left.getN());
                else
                    type_res = std::make_shared<DataTypeString>();
                return true;
            }
            else if constexpr (std::is_same_v<LeftDataType, DataTypeInterval> || std::is_same_v<RightDataType, DataTypeInterval>)
            {
                if constexpr (std::is_same_v<LeftDataType, DataTypeInterval> &&
                              std::is_same_v<RightDataType, DataTypeInterval>)
                {
                    if constexpr (is_plus || is_minus)
                    {
                        if (left.getKind() == right.getKind())
                        {
                            type_res = std::make_shared<LeftDataType>(left.getKind());
                            return true;
                        }
                    }
                }
            }
            else
            {
                if constexpr ((std::is_same_v<LeftDataType, DataTypeBFloat16> || std::is_same_v<RightDataType, DataTypeBFloat16>)
                    && (sizeof(typename LeftDataType::FieldType) > 8 || sizeof(typename RightDataType::FieldType) > 8))
                {
                    /// Big integers and BFloat16 are not supported together.
                    return false;
                }

                using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;


                if constexpr (!std::is_same_v<ResultDataType, InvalidType>)
                {
                    if constexpr (is_int_div || is_int_div_or_zero)
                        type_res = std::make_shared<ResultDataType>();
                    else if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                    {
                        if constexpr (is_division)
                        {
                            if (decimalCheckArithmeticOverflow(context))
                            {
                                /// Check overflow by using operands scale (based on big decimal division implementation details):
                                /// big decimal arithmetic is based on big integers, decimal operands are converted to big integers
                                /// i.e. int_operand = decimal_operand*10^scale
                                /// For division, left operand will be scaled by right operand scale also to do big integer division,
                                /// BigInt result = left*10^(left_scale + right_scale) / right * 10^right_scale
                                /// So, we can check upfront possible overflow just by checking max scale used for left operand
                                /// Note: it doesn't detect all possible overflow during big decimal division
                                if (left.getScale() + right.getScale() > ResultDataType::maxPrecision())
                                    throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Overflow during decimal division");
                            }
                        }
                        ResultDataType result_type = decimalResultType<is_multiply, is_division>(left, right);
                        type_res = std::make_shared<ResultDataType>(result_type.getPrecision(), result_type.getScale());
                    }
                    else if constexpr (((IsDataTypeDecimal<LeftDataType> && IsFloatingPoint<RightDataType>) ||
                        (IsDataTypeDecimal<RightDataType> && IsFloatingPoint<LeftDataType>)))
                    {
                        type_res = std::make_shared<DataTypeFloat64>();
                    }
                    else if constexpr (IsDataTypeDecimal<LeftDataType>)
                    {
                        type_res = std::make_shared<LeftDataType>(left.getPrecision(), left.getScale());
                    }
                    else if constexpr (IsDataTypeDecimal<RightDataType>)
                    {
                        type_res = std::make_shared<RightDataType>(right.getPrecision(), right.getScale());
                    }
                    else if constexpr (std::is_same_v<ResultDataType, DataTypeDateTime>)
                    {
                        // Special case for DateTime: binary OPS should reuse timezone
                        // of DateTime argument as timezeone of result type.
                        // NOTE: binary plus/minus are not allowed on DateTime64, and we are not handling it here.

                        const TimezoneMixin * tz = nullptr;
                        if constexpr (std::is_same_v<RightDataType, DataTypeDateTime>)
                            tz = &right;
                        if constexpr (std::is_same_v<LeftDataType, DataTypeDateTime>)
                            tz = &left;
                        type_res = std::make_shared<ResultDataType>(*tz);
                    }
                    else if constexpr (std::is_same_v<ResultDataType, DataTypeTime>)
                    {
                        type_res = std::make_shared<DataTypeTime>();
                    }
                    else
                        type_res = std::make_shared<ResultDataType>();
                    return true;
                }
            }
            return false;
        });

        if (valid)
        {
            if constexpr (is_division_or_null)
                return makeNullable(type_res);
            else
                return type_res;
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal types {} and {} of arguments of function {}",
            arguments[0]->getName(), arguments[1]->getName(), String(name));
    }

    ColumnPtr executeFixedString(const ColumnsWithTypeAndName & arguments) const
    {
        using OpImpl = FixedStringOperationImpl<Op<UInt8, UInt8>>;
        using OpReduceImpl = FixedStringReduceOperationImpl<Op<UInt8, UInt8>>;

        const auto * const col_left_raw = arguments[0].column.get();
        const auto * const col_right_raw = arguments[1].column.get();

        if (const auto * col_left_const = checkAndGetColumnConst<ColumnFixedString>(col_left_raw))
        {
            if (const auto * col_right_const = checkAndGetColumnConst<ColumnFixedString>(col_right_raw))
            {
                const auto * col_left = &checkAndGetColumn<ColumnFixedString>(col_left_const->getDataColumn());
                const auto * col_right = &checkAndGetColumn<ColumnFixedString>(col_right_const->getDataColumn());

                if (col_left->getN() != col_right->getN())
                    return nullptr;

                if constexpr (is_bit_hamming_distance)
                {
                    auto col_res = ColumnUInt16::create();
                    auto & data = col_res->getData();
                    data.resize_fill(col_left->size());

                    OpReduceImpl::template process<OpCase::Vector>(
                        col_left->getChars().data(), col_right->getChars().data(), data.data(), data.size(), col_left->getN());

                    return ColumnConst::create(std::move(col_res), col_left_raw->size());
                }
                else
                {
                    auto col_res = ColumnFixedString::create(col_left->getN());
                    auto & out_chars = col_res->getChars();

                    out_chars.resize(col_left->getN());

                    OpImpl::template process<OpCase::Vector>(
                        col_left->getChars().data(), col_right->getChars().data(), out_chars.data(), out_chars.size(), {});

                    return ColumnConst::create(std::move(col_res), col_left_raw->size());
                }

            }
        }

        const bool is_left_column_const = checkAndGetColumnConst<ColumnFixedString>(col_left_raw) != nullptr;
        const bool is_right_column_const = checkAndGetColumnConst<ColumnFixedString>(col_right_raw) != nullptr;

        const auto * col_left = is_left_column_const
                        ? checkAndGetColumn<ColumnFixedString>(
                            &checkAndGetColumnConst<ColumnFixedString>(col_left_raw)->getDataColumn())
                        : checkAndGetColumn<ColumnFixedString>(col_left_raw);
        const auto * col_right = is_right_column_const
                        ? checkAndGetColumn<ColumnFixedString>(
                            &checkAndGetColumnConst<ColumnFixedString>(col_right_raw)->getDataColumn())
                        : checkAndGetColumn<ColumnFixedString>(col_right_raw);

        if (col_left && col_right)
        {
            if (col_left->getN() != col_right->getN())
                return nullptr;

            if constexpr (is_bit_hamming_distance)
            {
                auto col_res = ColumnUInt16::create();
                auto & data = col_res->getData();
                data.resize_fill(is_right_column_const ? col_left->size() : col_right->size());

                if (!is_left_column_const && !is_right_column_const)
                {
                    OpReduceImpl::template process<OpCase::Vector>(
                        col_left->getChars().data(), col_right->getChars().data(), data.data(), data.size(), col_left->getN());
                }
                else if (is_left_column_const)
                {
                    OpReduceImpl::template process<OpCase::LeftConstant>(
                        col_left->getChars().data(), col_right->getChars().data(), data.data(), data.size(), col_left->getN());
                }
                else
                {
                    OpReduceImpl::template process<OpCase::RightConstant>(
                        col_left->getChars().data(), col_right->getChars().data(), data.data(), data.size(), col_left->getN());
                }

                return col_res;
            }
            else
            {
                auto col_res = ColumnFixedString::create(col_left->getN());
                auto & out_chars = col_res->getChars();
                out_chars.resize((is_right_column_const ? col_left->size() : col_right->size()) * col_left->getN());

                if (!is_left_column_const && !is_right_column_const)
                {
                    OpImpl::template process<OpCase::Vector>(
                        col_left->getChars().data(), col_right->getChars().data(), out_chars.data(), out_chars.size(), {});
                }
                else if (is_left_column_const)
                {
                    OpImpl::template process<OpCase::LeftConstant>(
                        col_left->getChars().data(), col_right->getChars().data(), out_chars.data(), out_chars.size(), col_left->getN());
                }
                else
                {
                    OpImpl::template process<OpCase::RightConstant>(
                        col_left->getChars().data(), col_right->getChars().data(), out_chars.data(), out_chars.size(), col_left->getN());
                }

                return col_res;
            }
        }
        return nullptr;
    }

    /// Only used for bitHammingDistance
    ColumnPtr executeString(const ColumnsWithTypeAndName & arguments) const
    {
        using OpImpl = StringReduceOperationImpl<Op<UInt8, UInt8>>;

        const auto * const col_left_raw = arguments[0].column.get();
        const auto * const col_right_raw = arguments[1].column.get();

        if (const auto * col_left_const = checkAndGetColumnConst<ColumnString>(col_left_raw))
        {
            if (const auto * col_right_const = checkAndGetColumnConst<ColumnString>(col_right_raw))
            {
                const auto * col_left = &checkAndGetColumn<ColumnString>(col_left_const->getDataColumn());
                const auto * col_right = &checkAndGetColumn<ColumnString>(col_right_const->getDataColumn());

                std::string_view a = col_left->getDataAt(0);
                std::string_view b = col_right->getDataAt(0);

                auto res = OpImpl::constConst(a, b);

                return DataTypeUInt64{}.createColumnConst(col_left_const->size(), res);
            }
        }

        const bool is_left_column_const = checkAndGetColumnConst<ColumnString>(col_left_raw) != nullptr;
        const bool is_right_column_const = checkAndGetColumnConst<ColumnString>(col_right_raw) != nullptr;

        const auto * col_left = is_left_column_const
            ? &checkAndGetColumn<ColumnString>(checkAndGetColumnConst<ColumnString>(col_left_raw)->getDataColumn())
            : checkAndGetColumn<ColumnString>(col_left_raw);
        const auto * col_right = is_right_column_const
            ? &checkAndGetColumn<ColumnString>(checkAndGetColumnConst<ColumnString>(col_right_raw)->getDataColumn())
            : checkAndGetColumn<ColumnString>(col_right_raw);

        if (col_left && col_right)
        {
            auto col_res = ColumnUInt64::create();
            auto & data = col_res->getData();
            data.resize(is_right_column_const ? col_left->size() : col_right->size());

            if (!is_left_column_const && !is_right_column_const)
            {
                OpImpl::vectorVector(
                    col_left->getChars(), col_left->getOffsets(), col_right->getChars(), col_right->getOffsets(), data);
            }
            else if (is_left_column_const)
            {
                std::string_view str_view = col_left->getDataAt(0);
                OpImpl::vectorConstant(col_right->getChars(), col_right->getOffsets(), str_view, data);
            }
            else
            {
                std::string_view str_view = col_right->getDataAt(0);
                OpImpl::vectorConstant(col_left->getChars(), col_left->getOffsets(), str_view, data);
            }

            return col_res;
        }
        return nullptr;
    }

template <typename LeftColumnType, typename A, typename B>
ColumnPtr executeStringInteger(const ColumnsWithTypeAndName & arguments, const A & left, const B & right) const
    {
        using LeftDataType = std::decay_t<decltype(left)>;
        using RightDataType = std::decay_t<decltype(right)>;

        const auto * const col_left_raw = arguments[0].column.get();
        const auto * const col_right_raw = arguments[1].column.get();
        using T1 = typename RightDataType::FieldType;

        using ColVecT1 = ColumnVector<T1>;
        const ColVecT1 * const col_right = checkAndGetColumn<ColVecT1>(col_right_raw);
        const ColumnConst * const col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw);

        using OpImpl = StringIntegerOperationImpl<T1, Op<LeftDataType, T1>>;

        const ColumnConst * const col_left_const = checkAndGetColumnConst<LeftColumnType>(col_left_raw);

        const auto * col_left = col_left_const ? &checkAndGetColumn<LeftColumnType>(col_left_const->getDataColumn())
                                               : checkAndGetColumn<LeftColumnType>(col_left_raw);

        if (!col_left)
            return nullptr;

        const typename LeftColumnType::Chars & in_vec = col_left->getChars();

        typename LeftColumnType::MutablePtr col_res;
        if constexpr (std::is_same_v<LeftDataType, DataTypeFixedString>)
            col_res = LeftColumnType::create(col_left->getN());
        else
            col_res = LeftColumnType::create();

        typename LeftColumnType::Chars & out_vec = col_res->getChars();

        if (col_left_const && col_right_const)
        {
            const T1 value = col_right_const->template getValue<T1>();
            if constexpr (std::is_same_v<LeftDataType, DataTypeFixedString>)
            {
                OpImpl::template processFixedString<OpCase::Vector>(in_vec.data(), col_left->getN(), &value, out_vec, 1);
            }
            else
            {
                ColumnString::Offsets & out_offsets = col_res->getOffsets();
                OpImpl::template processString<OpCase::Vector>(in_vec.data(), col_left->getOffsets().data(), &value, out_vec, out_offsets, 1);
            }

            return ColumnConst::create(std::move(col_res), col_left_const->size());
        }
        if (!col_left_const && !col_right_const && col_right)
        {
            if constexpr (std::is_same_v<LeftDataType, DataTypeFixedString>)
            {
                OpImpl::template processFixedString<OpCase::Vector>(
                    in_vec.data(), col_left->getN(), col_right->getData().data(), out_vec, col_left->size());
            }
            else
            {
                ColumnString::Offsets & out_offsets = col_res->getOffsets();
                out_offsets.reserve(col_left->size());
                OpImpl::template processString<OpCase::Vector>(
                    in_vec.data(), col_left->getOffsets().data(), col_right->getData().data(), out_vec, out_offsets, col_left->size());
            }
        }
        else if (col_left_const && col_right)
        {
            if constexpr (std::is_same_v<LeftDataType, DataTypeFixedString>)
            {
                OpImpl::template processFixedString<OpCase::LeftConstant>(
                    in_vec.data(), col_left->getN(), col_right->getData().data(), out_vec, col_right->size());
            }
            else
            {
                ColumnString::Offsets & out_offsets = col_res->getOffsets();
                out_offsets.reserve(col_right->size());
                OpImpl::template processString<OpCase::LeftConstant>(
                    in_vec.data(), col_left->getOffsets().data(), col_right->getData().data(), out_vec, out_offsets, col_right->size());
            }
        }
        else if (col_right_const)
        {
            const T1 value = col_right_const->template getValue<T1>();
            if constexpr (std::is_same_v<LeftDataType, DataTypeFixedString>)
            {
                OpImpl::template processFixedString<OpCase::RightConstant>(
                    in_vec.data(), col_left->getN(), &value, out_vec, col_left->size());
            }
            else
            {
                ColumnString::Offsets & out_offsets = col_res->getOffsets();
                out_offsets.reserve(col_left->size());
                OpImpl::template processString<OpCase::RightConstant>(
                    in_vec.data(), col_left->getOffsets().data(), &value, out_vec, out_offsets, col_left->size());
            }
        }
        else
            return nullptr;

        return col_res;
    }

    template <typename A, typename B>
    ColumnPtr executeNumeric(const ColumnsWithTypeAndName & arguments, const A & left, const B & right, const NullMap * right_nullmap) const
    {
        using LeftDataType = std::decay_t<decltype(left)>;
        using RightDataType = std::decay_t<decltype(right)>;
        using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;
        using DecimalResultType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::DecimalResultDataType;

        if constexpr (std::is_same_v<ResultDataType, InvalidType>)
        {
            return nullptr;
        }
        else if constexpr ((std::is_same_v<LeftDataType, DataTypeBFloat16> || std::is_same_v<RightDataType, DataTypeBFloat16>)
            && (sizeof(typename LeftDataType::FieldType) > 8 || sizeof(typename RightDataType::FieldType) > 8))
        {
            /// Big integers and BFloat16 are not supported together.
            return nullptr;
        }
        else // we can't avoid the else because otherwise the compiler may assume the ResultDataType may be Invalid
             // and that would produce the compile error.
        {
            constexpr bool decimal_with_float = (IsDataTypeDecimal<LeftDataType> && IsFloatingPoint<RightDataType>)
                || (IsFloatingPoint<LeftDataType> && IsDataTypeDecimal<RightDataType>);

            using T0 = std::conditional_t<decimal_with_float, Float64, typename LeftDataType::FieldType>;
            using T1 = std::conditional_t<decimal_with_float, Float64, typename RightDataType::FieldType>;
            using ResultType = typename ResultDataType::FieldType;
            using ColVecT0 = ColumnVectorOrDecimal<T0>;
            using ColVecT1 = ColumnVectorOrDecimal<T1>;
            using ColVecResult = ColumnVectorOrDecimal<ResultType>;

            ColumnPtr left_col = nullptr;
            ColumnPtr right_col = nullptr;

            /// When Decimal op Float32/64/16, convert both of them into Float64
            if constexpr (decimal_with_float)
            {
                const auto converted_type = std::make_shared<DataTypeFloat64>();
                left_col = castColumn(arguments[0], converted_type);
                right_col = castColumn(arguments[1], converted_type);
            }
            else
            {
                left_col = arguments[0].column;
                right_col = arguments[1].column;
            }
            const auto * const col_left_raw = left_col.get();
            const auto * const col_right_raw = right_col.get();

            const size_t col_left_size = col_left_raw->size();

            const ColumnConst * const col_left_const = checkAndGetColumnConst<ColVecT0>(col_left_raw);
            const ColumnConst * const col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw);

            const ColVecT0 * const col_left = checkAndGetColumn<ColVecT0>(col_left_raw);
            const ColVecT1 * const col_right = checkAndGetColumn<ColVecT1>(col_right_raw);

            if constexpr (IsDataTypeDecimal<ResultDataType>)
            {
                return executeNumericWithDecimal<LeftDataType, RightDataType, ResultDataType>(
                    left, right,
                    col_left_const, col_right_const,
                    col_left, col_right,
                    col_left_size,
                    right_nullmap);
            }
            /// Here we check if we have `intDiv` or `intDivOrZero` and at least one of the arguments is decimal, because in this case originally we had result as decimal, so we need to convert result into integer after calculations
            else if constexpr (!decimal_with_float && (is_int_div || is_int_div_or_zero) && (IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>))
            {
                if constexpr (!std::is_same_v<DecimalResultType, InvalidType>)
                {
                    DataTypePtr type_res;
                    if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                    {
                        DecimalResultType result_type = decimalResultType<is_multiply, is_division>(left, right);
                        type_res = std::make_shared<DecimalResultType>(result_type.getPrecision(), result_type.getScale());
                    }
                    else if constexpr (IsDataTypeDecimal<LeftDataType>)
                        type_res = std::make_shared<LeftDataType>(left.getPrecision(), left.getScale());
                    else
                        type_res = std::make_shared<RightDataType>(right.getPrecision(), right.getScale());

                    auto res = executeNumericWithDecimal<LeftDataType, RightDataType, DecimalResultType>(
                            left, right,
                            col_left_const, col_right_const,
                            col_left, col_right,
                            col_left_size,
                            right_nullmap);

                    auto col = ColumnWithTypeAndName(res, type_res, name);
                    return castColumn(col, std::make_shared<ResultDataType>());
                }
                return nullptr;
            }
            else // can't avoid else and another indentation level, otherwise the compiler would try to instantiate
                 // ColVecResult for Decimals which would lead to a compile error.
            {
                using OpImpl = BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>;

                /// non-vector result
                if (col_left_const && col_right_const)
                {
                    const auto res = right_nullmap && (*right_nullmap)[0] ? ResultType() : OpImpl::process(
                        col_left_const->template getValue<T0>(),
                        col_right_const->template getValue<T1>());

                    return ResultDataType().createColumnConst(col_left_const->size(), toField(res));
                }

                typename ColVecResult::MutablePtr col_res = ColVecResult::create();

                auto & vec_res = col_res->getData();
                vec_res.resize(col_left_size);

                if (col_left && col_right)
                {
                    OpImpl::template process<OpCase::Vector>(
                        col_left->getData().data(),
                        col_right->getData().data(),
                        vec_res.data(),
                        vec_res.size(),
                        right_nullmap);
                }
                else if (col_left_const && col_right)
                {
                    const T0 value = col_left_const->template getValue<T0>();

                    OpImpl::template process<OpCase::LeftConstant>(
                        &value,
                        col_right->getData().data(),
                        vec_res.data(),
                        vec_res.size(),
                        right_nullmap);
                }
                else if (col_left && col_right_const)
                {
                    const T1 value = col_right_const->template getValue<T1>();

                    OpImpl::template process<OpCase::RightConstant>(
                        col_left->getData().data(), &value, vec_res.data(), vec_res.size(), right_nullmap);
                }
                else
                    return nullptr;

                return col_res;
            }
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Special case when multiply aggregate function state
        if (isAggregateMultiply(arguments[0].type, arguments[1].type))
        {
            return executeAggregateMultiply(arguments, result_type, input_rows_count);
        }

        /// Special case - addition of two aggregate functions states
        if (isAggregateAddition(arguments[0].type, arguments[1].type))
        {
            return executeAggregateAddition(arguments, result_type, input_rows_count);
        }

        /// Special case when the function is minus, both arguments are DateTime64.
        if (isDateTime64Subtraction(arguments[0].type, arguments[1].type))
        {
            return executeDateTime64Subtraction(arguments, result_type, input_rows_count);
        }
        /// Special case when the function is minus, both arguments are Time64.
        if (isTime64Subtraction(arguments[0].type, arguments[1].type))
        {
            return executeTime64Subtraction(arguments, result_type, input_rows_count);
        }

        /// Special case when the function is plus, one argument is Date/Date32 and the other is Time/Time64.
        if (isDateAndTimeAddition(arguments[0].type, arguments[1].type))
        {
            return executeDateAndTimeAddition(arguments, result_type, input_rows_count);
        }

        /// Special case when the function is plus or minus, one of arguments is Date/DateTime/String and another is Interval.
        if (auto function_builder = getFunctionForIntervalArithmetic(arguments[0].type, arguments[1].type, context))
        {
            return executeDateTimeIntervalPlusMinus(arguments, result_type, input_rows_count, function_builder);
        }

        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Tuple.
        if (auto function_builder = getFunctionForDateTupleOfIntervalsArithmetic(arguments[0].type, arguments[1].type, context))
        {
            return executeDateTimeTupleOfIntervalsPlusMinus(arguments, result_type, input_rows_count, function_builder);
        }

        /// Special case when the function is plus or minus, one of arguments is Interval/Tuple of Intervals and another is Interval.
        if (auto function_builder = getFunctionForMergeIntervalsArithmetic(arguments[0].type, arguments[1].type, context))
        {
            return executeIntervalTupleOfIntervalsPlusMinus(arguments, result_type, input_rows_count, function_builder);
        }

        /// Special case when the function is plus, minus or multiply, both arguments are tuples.
        if (auto function_builder = getFunctionForTupleArithmetic(arguments[0].type, arguments[1].type, context))
        {
            return function_builder->build(arguments)->execute(arguments, result_type, input_rows_count, /* dry_run = */ false);
        }

        /// Special case when the function is multiply or divide, one of arguments is Tuple and another is Number.
        if (auto function_builder = getFunctionForTupleAndNumberArithmetic(arguments[0].type, arguments[1].type, context))
        {
            return executeTupleNumberOperator(arguments, result_type, input_rows_count, function_builder);
        }

        return executeImpl2(arguments, result_type, input_rows_count);
    }

    ColumnPtr executeImpl2(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, const NullMap * right_nullmap = nullptr) const
    {
        const auto & left_argument = arguments[0];
        const auto & right_argument = arguments[1];

        /// Process special case when operation is divide, intDiv or modulo and denominator
        /// is Nullable(Something) to prevent division by zero error.
        if (division_by_nullable && !right_nullmap)
        {
            assert(right_argument.type->isNullable());
            bool is_const = checkColumnConst<ColumnNullable>(right_argument.column.get());
            const ColumnNullable * nullable_column = is_const ? checkAndGetColumnConstData<ColumnNullable>(right_argument.column.get())
                                                              : checkAndGetColumn<ColumnNullable>(right_argument.column.get());

            const auto & null_bytemap = nullable_column->getNullMapData();
            auto res = executeImpl2(createBlockWithNestedColumns(arguments), removeNullable(result_type), input_rows_count, &null_bytemap);
            return wrapInNullable(res, arguments, result_type, input_rows_count);
        }

        /// Process special case when operation is divideOrNull, intDivOrNull, moduloOrNull or positiveModuloOrNull.
        if (is_division_or_null)
        {
            if (left_argument.column->onlyNull() || right_argument.column->onlyNull())
            {
                auto res = removeNullable(result_type)->createColumn();
                res->insertManyDefaults(input_rows_count);
                auto null_map_col = ColumnUInt8::create(input_rows_count, true);
                return !null_map_col->empty() ? wrapInNullable(std::move(res), std::move(null_map_col)) : makeNullable(std::move(res));
            }
            else if (result_type->isNullable())
            {
                auto null_map_col = ColumnUInt8::create(input_rows_count, false);
                PaddedPODArray<UInt8> & null_map_data = null_map_col->getData();
                for (size_t i = 0; i < input_rows_count; ++i)
                    null_map_data[i] = left_argument.column->isNullAt(i) || !right_argument.column->getBool(i);
                auto res = executeImpl2(createBlockWithNestedColumns(arguments), removeNullable(result_type), input_rows_count, right_nullmap);
                return !null_map_col->empty() ? wrapInNullable(res, std::move(null_map_col)) : makeNullable(res);
            }
        }

        /// Special case - one or both arguments are IPv4
        if (isIPv4(arguments[0].type) || isIPv4(arguments[1].type))
        {
            ColumnsWithTypeAndName new_arguments {
                {
                    isIPv4(arguments[0].type) ? castColumn(arguments[0], std::make_shared<DataTypeUInt32>()) : arguments[0].column,
                    isIPv4(arguments[0].type) ? std::make_shared<DataTypeUInt32>() : arguments[0].type,
                    arguments[0].name,
                },
                {
                    isIPv4(arguments[1].type) ? castColumn(arguments[1], std::make_shared<DataTypeUInt32>()) : arguments[1].column,
                    isIPv4(arguments[1].type) ? std::make_shared<DataTypeUInt32>() : arguments[1].type,
                    arguments[1].name
                }
            };

            return executeImpl2(new_arguments, result_type, input_rows_count, right_nullmap);
        }

        /// Special case - one or both arguments are IPv6
        if (isIPv6(arguments[0].type) || isIPv6(arguments[1].type))
        {
            ColumnsWithTypeAndName new_arguments {
                {
                    isIPv6(arguments[0].type) ? castColumn(arguments[0], std::make_shared<DataTypeUInt128>()) : arguments[0].column,
                    isIPv6(arguments[0].type) ? std::make_shared<DataTypeUInt128>() : arguments[0].type,
                    arguments[0].name,
                },
                {
                    isIPv6(arguments[1].type) ? castColumn(arguments[1], std::make_shared<DataTypeUInt128>()) : arguments[1].column,
                    isIPv6(arguments[1].type) ? std::make_shared<DataTypeUInt128>() : arguments[1].type,
                    arguments[1].name
                }
            };

            return executeImpl2(new_arguments, result_type, input_rows_count, right_nullmap);
        }

        /// Special case - Decimal op Float (or Float op Decimal): both sides are converted to
        /// Float64 regardless of the specific Decimal/Float widths. Handle at runtime to avoid
        /// instantiating 4 Decimal × 3 Float × 2 directions = 24 redundant template specializations
        /// that all collapse to the same Float64 × Float64 code path.
        /// Exclude intDiv/intDivOrZero/intDivOrNull: their result type is an integer whose width
        /// depends on the original operand types (e.g. Decimal32 → Int32), not Float64.
        if constexpr (IsOperation<Op>::allow_decimal && valid_on_float_arguments
            && !IsOperation<Op>::int_div && !IsOperation<Op>::int_div_or_zero && !IsOperation<Op>::int_div_or_null)
        {
            const WhichDataType left_which(left_argument.type);
            const WhichDataType right_which(right_argument.type);

            if ((left_which.isDecimal() && right_which.isFloat()) || (left_which.isFloat() && right_which.isDecimal()))
            {
                const auto float64_type = std::make_shared<DataTypeFloat64>();
                ColumnsWithTypeAndName new_arguments
                {
                    {castColumn(arguments[0], float64_type), float64_type, arguments[0].name},
                    {castColumn(arguments[1], float64_type), float64_type, arguments[1].name},
                };
                return executeImpl2(new_arguments, result_type, input_rows_count, right_nullmap);
            }
        }

        const auto * const left_generic = left_argument.type.get();
        const auto * const right_generic = right_argument.type.get();
        ColumnPtr res;

        const bool valid = castBothTypes(left_generic, right_generic, [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;

            if constexpr ((std::is_same_v<DataTypeFixedString, LeftDataType> || std::is_same_v<DataTypeString, LeftDataType>) ||
                          (std::is_same_v<DataTypeFixedString, RightDataType> || std::is_same_v<DataTypeString, RightDataType>))
            {
                if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType> &&
                              std::is_same_v<DataTypeFixedString, RightDataType>)
                {
                    if constexpr (!Op<DataTypeFixedString, DataTypeFixedString>::allow_fixed_string)
                        return false;
                    else
                        return (res = executeFixedString(arguments)) != nullptr;
                }

                if constexpr (
                    is_bit_hamming_distance
                    && std::is_same_v<DataTypeString, LeftDataType> && std::is_same_v<DataTypeString, RightDataType>)
                    return (res = executeString(arguments)) != nullptr;
                else if constexpr (!Op<LeftDataType, RightDataType>::allow_string_integer)
                    return false;
                else if constexpr (!IsIntegral<RightDataType>)
                    return false;
                else if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType>)
                {
                    return (res = executeStringInteger<ColumnFixedString>(arguments, left, right)) != nullptr;
                }
                else if constexpr (std::is_same_v<DataTypeString, LeftDataType>)
                    return (res = executeStringInteger<ColumnString>(arguments, left, right)) != nullptr;
            }
            else if constexpr (
                IsOperation<Op>::allow_decimal && valid_on_float_arguments
                && !IsOperation<Op>::int_div && !IsOperation<Op>::int_div_or_zero && !IsOperation<Op>::int_div_or_null
                && ((IsDataTypeDecimal<LeftDataType> && IsFloatingPoint<RightDataType>)
                    || (IsFloatingPoint<LeftDataType> && IsDataTypeDecimal<RightDataType>)))
            {
                /// Decimal × Float pairs are handled at runtime above (converted to Float64 × Float64).
                /// Skip them here to avoid redundant template instantiations.
                return false;
            }
            else
                return (res = executeNumeric(arguments, left, right, right_nullmap)) != nullptr;
        });

        if (isArray(result_type))
        {
            if (!isArray(arguments[0].type) || !isArray(arguments[1].type))
                return executeArrayWithNumericImpl(arguments, result_type, input_rows_count);
            return executeArraysImpl(arguments, result_type, input_rows_count);
        }

        if (!valid)
        {
            // This is a logical error, because the types should have been checked
            // by getReturnTypeImpl().
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Arguments of '{}' have incorrect data types: '{}' of type '{}',"
                " '{}' of type '{}'", getName(),
                left_argument.name, left_argument.type->getName(),
                right_argument.name, right_argument.type->getName());
        }

        return res;
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & arguments, const DataTypePtr & result_type) const override
    {
        if (2 != arguments.size())
            return false;

        if (!canBeNativeType(*arguments[0]) || !canBeNativeType(*arguments[1]) || !canBeNativeType(*result_type))
            return false;

        auto denull_left_type = removeNullable(arguments[0]);
        auto denull_right_type = removeNullable(arguments[1]);
        WhichDataType data_type_lhs(denull_left_type);
        WhichDataType data_type_rhs(denull_right_type);
        if ((data_type_lhs.isDateOrDate32() || data_type_lhs.isDateTime() || data_type_lhs.isTime()) ||
            (data_type_rhs.isDateOrDate32() || data_type_rhs.isDateTime() || data_type_rhs.isTime()))
            return false;

        return castBothTypes(
            denull_left_type.get(),
            denull_right_type.get(),
            [&](const auto & left, const auto & right)
            {
                using LeftDataType = std::decay_t<decltype(left)>;
                using RightDataType = std::decay_t<decltype(right)>;
                if constexpr (
                    !std::is_same_v<DataTypeFixedString, LeftDataType> && !std::is_same_v<DataTypeFixedString, RightDataType>
                    && !std::is_same_v<DataTypeString, LeftDataType> && !std::is_same_v<DataTypeString, RightDataType>)
                {
                    using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;
                    using OpSpec = Op<typename LeftDataType::FieldType, typename RightDataType::FieldType>;

                    if constexpr (
                        !std::is_same_v<ResultDataType, InvalidType> && !IsDataTypeDecimal<ResultDataType>
                        && !IsDataTypeDecimal<LeftDataType> && !IsDataTypeDecimal<RightDataType> && OpSpec::compilable)
                    {
                        return true;
                    }
                }
                return false;
            });
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const override
    {
        assert(2 == arguments.size());

        auto denull_left_type = removeNullable(arguments[0].type);
        auto denull_right_type = removeNullable(arguments[1].type);
        llvm::Value * result = nullptr;

        castBothTypes(
            denull_left_type.get(),
            denull_right_type.get(),
            [&](const auto & left, const auto & right)
            {
                using LeftDataType = std::decay_t<decltype(left)>;
                using RightDataType = std::decay_t<decltype(right)>;
                if constexpr (
                    !std::is_same_v<DataTypeFixedString, LeftDataType> && !std::is_same_v<DataTypeFixedString, RightDataType>
                    && !std::is_same_v<DataTypeString, LeftDataType> && !std::is_same_v<DataTypeString, RightDataType>)
                {
                    using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;
                    using OpSpec = Op<typename LeftDataType::FieldType, typename RightDataType::FieldType>;
                    if constexpr (
                        !std::is_same_v<ResultDataType, InvalidType> && !IsDataTypeDecimal<ResultDataType>
                        && !IsDataTypeDecimal<LeftDataType> && !IsDataTypeDecimal<RightDataType> && OpSpec::compilable)
                    {
                        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
                        auto * lval = nativeCast(b, arguments[0], result_type);
                        auto * rval = nativeCast(b, arguments[1], result_type);
                        result = OpSpec::compile(b, lval, rval, std::is_signed_v<typename ResultDataType::FieldType>);
                        return true;
                    }
                }
                return false;
            });

        return result;
    }
#endif

    bool canBeExecutedOnDefaultArguments() const override { return valid_on_default_arguments; }
};


template <template <typename, typename> class Op, typename Name, bool valid_on_default_arguments = true, bool valid_on_float_arguments = true, bool division_by_nullable = false>
class FunctionBinaryArithmeticWithConstants : public FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments, valid_on_float_arguments, division_by_nullable>
{
public:
    using Base = FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments, valid_on_float_arguments, division_by_nullable>;
    using Monotonicity = typename Base::Monotonicity;

    static FunctionPtr create(
        const ColumnWithTypeAndName & left_,
        const ColumnWithTypeAndName & right_,
        const DataTypePtr & return_type_,
        ContextPtr context)
    {
        return std::make_shared<FunctionBinaryArithmeticWithConstants>(left_, right_, return_type_, context);
    }

    FunctionBinaryArithmeticWithConstants(
        const ColumnWithTypeAndName & left_,
        const ColumnWithTypeAndName & right_,
        const DataTypePtr & return_type_,
        ContextPtr context_)
        : Base(context_), left(left_), right(right_), return_type(return_type_)
    {
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (left.column && isColumnConst(*left.column) && arguments.size() == 1)
        {
            ColumnsWithTypeAndName columns_with_constant
                = {{left.column->cloneResized(input_rows_count), left.type, left.name},
                   arguments[0]};

            return Base::executeImpl(columns_with_constant, result_type, input_rows_count);
        }
        if (right.column && isColumnConst(*right.column) && arguments.size() == 1)
        {
            ColumnsWithTypeAndName columns_with_constant
                = {arguments[0], {right.column->cloneResized(input_rows_count), right.type, right.name}};

            return Base::executeImpl(columns_with_constant, result_type, input_rows_count);
        }
        return Base::executeImpl(arguments, result_type, input_rows_count);
    }

    bool hasInformationAboutMonotonicity() const override
    {
        const std::string_view name_view = Name::name;
        return (name_view == "minus" || name_view == "plus" || name_view == "multiply" || name_view == "divide" || name_view == "intDiv");
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field & left_point, const Field & right_point) const override
    {
        const std::string_view name_view = Name::name;

        // NaN breaks monotonicity for floating-point types.
        if (isNaNField(left_point) || isNaNField(right_point))
            return {false, true, false, false};

        // For simplicity, we treat null values as monotonicity breakers, except for variable / non-zero constant.
        if (left_point.isNull() || right_point.isNull())
        {
            if (name_view == "divide" || name_view == "intDiv")
            {
                // variable / constant
                if (right.column && isColumnConst(*right.column))
                {
                    auto constant = (*right.column)[0];
                    if (accurateEquals(constant, Field(0)))
                        return {false, true, false}; // variable / 0 is undefined, let's treat it as non-monotonic
                    bool is_constant_positive = accurateLess(Field(0), constant);

                    // division is saturated to `inf`, thus it doesn't have overflow issues.
                    return {true, is_constant_positive, true};
                }
            }
            return {false, true, false, false};
        }

        // For simplicity, we treat every single value interval as positive monotonic,
        // unless the function is undefined at that point (e.g. division by zero).
        if (accurateEquals(left_point, right_point))
        {
            // Division is undefined at zero, so we must not report monotonicity:
            // - divide(const, x) or intDiv(const, x) at x = 0 (right_arg_is_zero)
            // - divide(x, 0) or intDiv(x, 0) where the constant divisor is 0 (right_const_is_zero)
            bool is_div_function = name_view == "divide" || name_view == "intDiv";
            bool right_arg_is_zero = left.column && isColumnConst(*left.column) && accurateEquals(left_point, Field(0));
            bool right_const_is_zero = right.column && isColumnConst(*right.column) && accurateEquals((*right.column)[0], Field(0));

            if (is_div_function && (right_arg_is_zero || right_const_is_zero))
                return {false, true, false, false};

            return {true, true, false, false};
        }

        if (name_view == "minus" || name_view == "plus")
        {
            // const +|- variable
            if (left.column && isColumnConst(*left.column))
            {
                auto left_type = removeNullable(removeLowCardinality(left.type));
                auto right_type = removeNullable(removeLowCardinality(right.type));
                auto ret_type = removeNullable(removeLowCardinality(return_type));

                auto transform = [&](const Field & point)
                {
                    ColumnsWithTypeAndName columns_with_constant
                        = {{left_type->createColumnConst(1, (*left.column)[0]), left_type, left.name},
                           {right_type->createColumnConst(1, point), right_type, right.name}};

                    /// This is a bit dangerous to call Base::executeImpl cause it ignores `use Default Implementation For XXX` flags.
                    /// It was possible to check monotonicity for nullable right type which result to exception.
                    /// Adding removeNullable above fixes the issue, but some other inconsistency may left.
                    auto col = Base::executeImpl(columns_with_constant, ret_type, 1);
                    Field point_transformed;
                    col->get(0, point_transformed);
                    return point_transformed;
                };

                bool is_positive_monotonicity = accurateLess(left_point, right_point)
                            == accurateLess(transform(left_point), transform(right_point));

                if (name_view == "plus")
                {
                    // Check if there is an overflow
                    if (is_positive_monotonicity)
                        return {true, true, false, true};
                    return {false, true, false, false};
                }

                // Check if there is an overflow
                if (!is_positive_monotonicity)
                    return {true, false, false, true};
                return {false, false, false, false};
            }
            // variable +|- constant
            if (right.column && isColumnConst(*right.column))
            {
                auto left_type = removeNullable(removeLowCardinality(left.type));
                auto right_type = removeNullable(removeLowCardinality(right.type));
                auto ret_type = removeNullable(removeLowCardinality(return_type));

                auto transform = [&](const Field & point)
                {
                    ColumnsWithTypeAndName columns_with_constant
                        = {{left_type->createColumnConst(1, point), left_type, left.name},
                           {right_type->createColumnConst(1, (*right.column)[0]), right_type, right.name}};

                    auto col = Base::executeImpl(columns_with_constant, ret_type, 1);
                    Field point_transformed;
                    col->get(0, point_transformed);
                    return point_transformed;
                };

                // Check if there is an overflow
                if (accurateLess(left_point, right_point) == accurateLess(transform(left_point), transform(right_point)))
                    return {true, true, false, true};
                return {false, true, false, false};
            }
        }
        if (name_view == "divide" || name_view == "intDiv")
        {
            bool is_strict = name_view == "divide";

            // const / variable
            if (left.column && isColumnConst(*left.column))
            {
                auto constant = (*left.column)[0];
                if (accurateEquals(constant, Field(0)))
                    return {true, true, false, false}; // 0 / 0 is undefined, thus it's not always monotonic

                bool is_constant_positive = accurateLess(Field(0), constant);
                if (accurateLess(left_point, Field(0))
                    && accurateLess(right_point, Field(0)))
                {
                    return {true, is_constant_positive, false, is_strict};
                }
                if (accurateLess(Field(0), left_point)
                    && accurateLess(Field(0), right_point))
                {
                    return {true, !is_constant_positive, false, is_strict};
                }
            }
            // variable / constant
            else if (right.column && isColumnConst(*right.column))
            {
                auto constant = (*right.column)[0];
                if (accurateEquals(constant, Field(0)))
                    return {false, true, false, false}; // variable / 0 is undefined, let's treat it as non-monotonic

                bool is_constant_positive = accurateLess(Field(0), constant);
                // division is saturated to `inf`, thus it doesn't have overflow issues.
                return {true, is_constant_positive, true, is_strict};
            }
        }
        if (name_view == "multiply")
        {
            /// variable * constant or constant * variable
            const auto & const_side = (right.column && isColumnConst(*right.column)) ? right : left;
            if (const_side.column && isColumnConst(*const_side.column))
            {
                auto constant = (*const_side.column)[0];
                if (accurateEquals(constant, Field(0)))
                    return {true, true, false, false}; /// x * 0 is constant, trivially monotonic but not strict

                auto ret_type = removeNullable(removeLowCardinality(return_type));

                bool is_constant_positive = accurateLess(Field(0), constant);

                /// Check if multiplication can overflow within [left_point, right_point].
                ///
                /// Multiply by constant `c` can have `c-1` wrap boundaries in modular arithmetic,
                /// so unlike plus/minus, comparing endpoint directions does not reliably detect overflow.
                /// Instead, we verify each endpoint is within [TYPE_MIN/c, TYPE_MAX/c].
                /// If both are in this safe range, the entire interval is overflow-free.

                auto check_overflow = [&]<typename T>(const Field & point, const Field & c, std::type_identity<T>) -> bool
                {
                    T type_min = std::numeric_limits<T>::min();
                    T type_max = std::numeric_limits<T>::max();

                    /// Convert Field to T, returning nullopt if the value is out of range.
                    auto to_T = [&](const Field & f) -> std::optional<T>
                    {
                        return Field::dispatch([&](const auto & v) -> std::optional<T>
                        {
                            using V = std::decay_t<decltype(v)>;
                            if constexpr (is_integer<V>)
                            {
                                if (accurate::lessOp(v, type_min) || accurate::greaterOp(v, type_max))
                                    return std::nullopt;
                                return static_cast<T>(v);
                            }
                            else
                                return std::nullopt;
                        }, f);
                    };

                    auto a = to_T(point);
                    auto b = to_T(c);
                    if (!a || !b)
                        return true; /// value doesn't fit → overflow

                    if (*b == T(0))
                        return false;

                    if constexpr (is_signed_v<T>)
                    {
                        /// TYPE_MIN / -1 is UB for native signed types.
                        if (*b == T(-1))
                            return *a == type_min;

                        T lo = (*b > T(0)) ? (type_min / *b) : (type_max / *b);
                        T hi = (*b > T(0)) ? (type_max / *b) : (type_min / *b);
                        return *a < lo || *a > hi;
                    }
                    else
                    {
                        return *a > type_max / *b;
                    }
                };

                auto overflows = [&]<typename T>(std::type_identity<T> tag)
                {
                    return check_overflow(left_point, constant, tag)
                        || check_overflow(right_point, constant, tag);
                };

                WhichDataType which(ret_type->getTypeId());
                bool overflow
                    = which.isUInt8()   ? overflows(std::type_identity<UInt8>{})
                    : which.isUInt16()  ? overflows(std::type_identity<UInt16>{})
                    : which.isUInt32()  ? overflows(std::type_identity<UInt32>{})
                    : which.isUInt64()  ? overflows(std::type_identity<UInt64>{})
                    : which.isUInt128() ? overflows(std::type_identity<UInt128>{})
                    : which.isUInt256() ? overflows(std::type_identity<UInt256>{})
                    : which.isInt8()    ? overflows(std::type_identity<Int8>{})
                    : which.isInt16()   ? overflows(std::type_identity<Int16>{})
                    : which.isInt32()   ? overflows(std::type_identity<Int32>{})
                    : which.isInt64()   ? overflows(std::type_identity<Int64>{})
                    : which.isInt128()  ? overflows(std::type_identity<Int128>{})
                    : which.isInt256()  ? overflows(std::type_identity<Int256>{})
                    : true; /// unknown type — conservatively assume overflow

                if (overflow)
                    return {false, true, false, false};

                return {true, is_constant_positive, false, true};
            }
        }
        return {false, true, false};
    }

private:
    ColumnWithTypeAndName left;
    ColumnWithTypeAndName right;
    DataTypePtr return_type;
};

template <template <typename, typename> class Op, typename Name, bool valid_on_default_arguments = true, bool valid_on_float_arguments = true>
class BinaryArithmeticOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = Name::name;
    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        return std::make_unique<BinaryArithmeticOverloadResolver>(context);
    }

    explicit BinaryArithmeticOverloadResolver(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return false; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        /// Only division-like operations can have division_by_nullable=true.
        /// Using if constexpr avoids instantiating FunctionBinaryArithmetic<..., true> and
        /// FunctionBinaryArithmeticWithConstants<..., true> for all other operations,
        /// significantly reducing template bloat.
        static constexpr bool can_have_division_by_nullable =
            IsOperation<Op>::int_div || IsOperation<Op>::modulo || IsOperation<Op>::positive_modulo || IsOperation<Op>::div_floating;

        bool division_by_nullable = false;
        if constexpr (can_have_division_by_nullable)
        {
            /// Check the case when operation is divide, intDiv or modulo and denominator is Nullable(Something).
            /// For divide operation we should check only Nullable(Decimal), because only this case can throw division by zero error.
            division_by_nullable = !arguments[0].type->onlyNull() && !arguments[1].type->onlyNull() && arguments[1].type->isNullable()
                && (IsOperation<Op>::int_div || IsOperation<Op>::modulo || IsOperation<Op>::positive_modulo
                    || (IsOperation<Op>::div_floating
                        && (isDecimalOrNullableDecimal(arguments[0].type) || isDecimalOrNullableDecimal(arguments[1].type))));
        }

        auto make_adaptor = [&](auto function)
        {
            return std::make_unique<FunctionToFunctionBaseAdaptor>(
                function,
                DataTypes{std::from_range_t{}, arguments | std::views::transform([](const auto & elem) { return elem.type; })},
                return_type);
        };

        /// More efficient specialization for two numeric arguments.
        if (arguments.size() == 2
            && ((arguments[0].column && isColumnConst(*arguments[0].column))
                || (arguments[1].column && isColumnConst(*arguments[1].column))))
        {
            if constexpr (can_have_division_by_nullable)
            {
                if (division_by_nullable)
                    return make_adaptor(FunctionBinaryArithmeticWithConstants<Op, Name, valid_on_default_arguments, valid_on_float_arguments, true>::create(
                        arguments[0], arguments[1], return_type, context));
            }
            return make_adaptor(FunctionBinaryArithmeticWithConstants<Op, Name, valid_on_default_arguments, valid_on_float_arguments, false>::create(
                arguments[0], arguments[1], return_type, context));
        }

        if constexpr (can_have_division_by_nullable)
        {
            if (division_by_nullable)
                return make_adaptor(FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments, valid_on_float_arguments, true>::create(context));
        }
        return make_adaptor(FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments, valid_on_float_arguments, false>::create(context));
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2",
                getName(), arguments.size());
        return FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments, valid_on_float_arguments>::getReturnTypeImplStatic(arguments, context);
    }

private:
    ContextPtr context;
};
}
