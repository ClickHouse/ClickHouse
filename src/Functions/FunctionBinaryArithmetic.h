#pragma once

// Include this first, because `#define _asan_poison_address` from
// llvm/Support/Compiler.h conflicts with its forward declaration in
// sanitizer/asan_interface.h
#include <memory>
#include <type_traits>
#include <base/wide_integer_to_string.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Native.h>
#include <DataTypes/NumberTraits.h>
#include <Functions/DivisionUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IsOperation.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/castColumn.h>
#include <base/TypeList.h>
#include <base/map.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>

#if USE_EMBEDDED_COMPILER
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wunused-parameter"
#    include <llvm/IR/IRBuilder.h>
#    pragma GCC diagnostic pop
#endif

#include <cassert>

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
}

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
template <> inline constexpr bool IsFloatingPoint<DataTypeFloat32> = true;
template <> inline constexpr bool IsFloatingPoint<DataTypeFloat64> = true;

template <typename DataType> constexpr bool IsDateOrDateTime = false;
template <> inline constexpr bool IsDateOrDateTime<DataTypeDate> = true;
template <> inline constexpr bool IsDateOrDateTime<DataTypeDateTime> = true;

template <typename T0, typename T1> constexpr bool UseLeftDecimal = false;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal256>, DataTypeDecimal<Decimal128>> = true;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal256>, DataTypeDecimal<Decimal64>> = true;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal256>, DataTypeDecimal<Decimal32>> = true;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal32>> = true;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal64>> = true;
template <> inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal64>, DataTypeDecimal<Decimal32>> = true;

template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct BinaryOperationTraits
{
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
private: /// it's not correct for Decimal
    using Op = Operation<T0, T1>;

public:
    static constexpr bool allow_decimal = IsOperation<Operation>::allow_decimal;

    /// Appropriate result type for binary operator on numeric types. "Date" can also mean
    /// DateTime, but if both operands are Dates, their type must be the same (e.g. Date - DateTime is invalid).
    using ResultDataType = Switch<
        /// Decimal cases
        Case<!allow_decimal && (IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>), InvalidType>,
        Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> && UseLeftDecimal<LeftDataType, RightDataType>, LeftDataType>,
        Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType>, RightDataType>,
        Case<IsDataTypeDecimal<LeftDataType> && IsIntegralOrExtended<RightDataType>, LeftDataType>,
        Case<IsDataTypeDecimal<RightDataType> && IsIntegralOrExtended<LeftDataType>, RightDataType>,

        /// e.g Decimal +-*/ Float, least(Decimal, Float), greatest(Decimal, Float) = Float64
        Case<IsOperation<Operation>::allow_decimal && IsDataTypeDecimal<LeftDataType> && IsFloatingPoint<RightDataType>,
            DataTypeFloat64>,
        Case<IsOperation<Operation>::allow_decimal && IsDataTypeDecimal<RightDataType> && IsFloatingPoint<LeftDataType>,
            DataTypeFloat64>,

        Case<IsOperation<Operation>::bit_hamming_distance && IsIntegral<LeftDataType> && IsIntegral<RightDataType>,
            DataTypeUInt8>,

        /// Decimal <op> Real is not supported (traditional DBs convert Decimal <op> Real to Real)
        Case<IsDataTypeDecimal<LeftDataType> && !IsIntegralOrExtendedOrDecimal<RightDataType>, InvalidType>,
        Case<IsDataTypeDecimal<RightDataType> && !IsIntegralOrExtendedOrDecimal<LeftDataType>, InvalidType>,

        /// number <op> number -> see corresponding impl
        Case<!IsDateOrDateTime<LeftDataType> && !IsDateOrDateTime<RightDataType>,
            DataTypeFromFieldType<typename Op::ResultType>>,

        /// Date + Integral -> Date
        /// Integral + Date -> Date
        Case<IsOperation<Operation>::plus, Switch<
            Case<IsIntegral<RightDataType>, LeftDataType>,
            Case<IsIntegral<LeftDataType>, RightDataType>>>,

        /// Date - Date     -> Int32
        /// Date - Integral -> Date
        Case<IsOperation<Operation>::minus, Switch<
            Case<std::is_same_v<LeftDataType, RightDataType>, DataTypeInt32>,
            Case<IsDateOrDateTime<LeftDataType> && IsIntegral<RightDataType>, LeftDataType>>>,

        /// least(Date, Date) -> Date
        /// greatest(Date, Date) -> Date
        Case<std::is_same_v<LeftDataType, RightDataType> && (IsOperation<Operation>::least || IsOperation<Operation>::greatest),
            LeftDataType>,

        /// Date % Int32 -> Int32
        /// Date % Float -> Float64
        Case<IsOperation<Operation>::modulo, Switch<
            Case<IsDateOrDateTime<LeftDataType> && IsIntegral<RightDataType>, RightDataType>,
            Case<IsDateOrDateTime<LeftDataType> && IsFloatingPoint<RightDataType>, DataTypeFloat64>>>>;
};
}

namespace impl_
{

/** Arithmetic operations: +, -, *, /, %,
  * intDiv (integer division)
  * Bitwise operations: |, &, ^, ~.
  * Etc.
  */

enum class OpCase { Vector, LeftConstant, RightConstant };

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
    static inline void apply(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t i)
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
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = true;

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
                Op::apply(&in_vec[0], &in_vec[in_offsets[0] - 1], b[i], out_vec, out_offsets);
            }
            else
            {
                size_t new_offset = in_offsets[i];

                if constexpr (op_case == OpCase::Vector)
                {
                    Op::apply(&in_vec[prev_offset], &in_vec[new_offset - 1], b[i], out_vec, out_offsets);
                }
                else
                {
                    Op::apply(&in_vec[prev_offset], &in_vec[new_offset - 1], b[0], out_vec, out_offsets);
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

        UInt8 b_repeated[b_repeated_size];

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
                        unwrap<op_case, OpCase::LeftConstant>(a, i),
                        unwrap<op_case, OpCase::RightConstant>(b, i),
                        scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(
                        unwrap<op_case, OpCase::LeftConstant>(a, i),
                        unwrap<op_case, OpCase::RightConstant>(b, i),
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
                        unwrap<op_case, OpCase::LeftConstant>(a, i),
                        unwrap<op_case, OpCase::RightConstant>(b, i),
                        scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false, false>(
                        unwrap<op_case, OpCase::LeftConstant>(a, i),
                        unwrap<op_case, OpCase::RightConstant>(b, i),
                        scale_b);
                return;
            }

        }
        else if constexpr (is_division && is_decimal_b)
        {
            processWithRightNullmapImpl<op_case>(a, b, c, size, right_nullmap, [&scale_a](const auto & left, const auto & right)
            {
                return applyScaledDiv<is_decimal_a>(left, right, scale_a);
            });
            return;
        }

        processWithRightNullmapImpl<op_case>(a, b, c, size, right_nullmap, [](const auto & left, const auto & right){ return apply(left, right); });
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
    static inline void processWithRightNullmapImpl(const auto & a, const auto & b, ResultContainerType & c, size_t size, const NullMap * right_nullmap, ApplyFunc apply_func)
    {
        if (right_nullmap)
        {
            if constexpr (op_case == OpCase::RightConstant)
            {
                if ((*right_nullmap)[0])
                    return;

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
    static constexpr bool is_int_division = IsOperation<Operation>::div_int ||
                                            IsOperation<Operation>::div_int_or_zero;
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
                throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
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
                throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
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
                    throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
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
    static constexpr const bool is_plus = IsOperation<Op>::plus;
    static constexpr const bool is_minus = IsOperation<Op>::minus;
    static constexpr const bool is_multiply = IsOperation<Op>::multiply;
    static constexpr const bool is_division = IsOperation<Op>::division;

    ContextPtr context;
    bool check_decimal_overflow = true;

    static bool castType(const IDataType * type, auto && f)
    {
        using Types = TypeList<
            DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256,
            DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64, DataTypeInt128, DataTypeInt256,
            DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128, DataTypeDecimal256,
            DataTypeDate, DataTypeDateTime,
            DataTypeFixedString, DataTypeString>;

        using Floats = TypeList<DataTypeFloat32, DataTypeFloat64>;

        using ValidTypes = std::conditional_t<valid_on_float_arguments,
            TypeListConcat<Types, Floats>,
            Types>;

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
        bool first_is_date_or_datetime = isDate(type0) || isDateTime(type0) || isDateTime64(type0);
        bool second_is_date_or_datetime = isDate(type1) || isDateTime(type1) || isDateTime64(type1);

        /// Exactly one argument must be Date or DateTime
        if (first_is_date_or_datetime == second_is_date_or_datetime)
            return {};

        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        /// We construct another function (example: addMonths) and call it.

        if constexpr (!is_plus && !is_minus)
            return {};

        const DataTypePtr & type_time = first_is_date_or_datetime ? type0 : type1;
        const DataTypePtr & type_interval = first_is_date_or_datetime ? type1 : type0;

        bool interval_is_number = isNumber(type_interval);

        const DataTypeInterval * interval_data_type = nullptr;
        if (!interval_is_number)
        {
            interval_data_type = checkAndGetDataType<DataTypeInterval>(type_interval.get());

            if (!interval_data_type)
                return {};
        }

        if (second_is_date_or_datetime && is_minus)
            throw Exception("Wrong order of arguments for function " + String(name) + ": argument of type Interval cannot be first",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        std::string function_name;
        if (interval_data_type)
        {
            function_name = fmt::format("{}{}s",
                is_plus ? "add" : "subtract",
                interval_data_type->getKind().toString());
        }
        else
        {
            if (isDate(type_time))
                function_name = is_plus ? "addDays" : "subtractDays";
            else
                function_name = is_plus ? "addSeconds" : "subtractSeconds";
        }

        return FunctionFactory::instance().get(function_name, context);
    }

    static FunctionOverloadResolverPtr
    getFunctionForTupleArithmetic(const DataTypePtr & type0, const DataTypePtr & type1, ContextPtr context)
    {
        if (!isTuple(type0) || !isTuple(type1))
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
        if (!(isTuple(type0) && isNumber(type1)) && !(isTuple(type1) && isNumber(type0)))
            return {};

        /// Special case when the function is multiply or divide, one of arguments is Tuple and another is Number.
        /// We construct another function (example: tupleMultiplyByNumber) and call it.

        if constexpr (!is_multiply && !is_division)
            return {};

        if (isNumber(type0) && is_division)
            throw Exception("Wrong order of arguments for function " + String(name) + ": argument of numeric type cannot be first",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        std::string function_name;
        if (is_multiply)
        {
            function_name = "tupleMultiplyByNumber";
        }
        else
        {
            function_name = "tupleDivideByNumber";
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

    /// Multiply aggregation state by integer constant: by merging it with itself specified number of times.
    ColumnPtr executeAggregateMultiply(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        ColumnsWithTypeAndName new_arguments = arguments;
        if (WhichDataType(new_arguments[1].type).isAggregateFunction())
            std::swap(new_arguments[0], new_arguments[1]);

        if (!isColumnConst(*new_arguments[1].column))
            throw Exception{"Illegal column " + new_arguments[1].column->getName()
                + " of argument of aggregation state multiply. Should be integer constant", ErrorCodes::ILLEGAL_COLUMN};

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
        else
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
        else
            return column_to;
    }

    ColumnPtr executeDateTimeIntervalPlusMinus(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                                               size_t input_rows_count, const FunctionOverloadResolverPtr & function_builder) const
    {
        ColumnsWithTypeAndName new_arguments = arguments;

        /// Interval argument must be second.
        if (isDate(arguments[1].type) || isDateTime(arguments[1].type) || isDateTime64(arguments[1].type))
            std::swap(new_arguments[0], new_arguments[1]);

        /// Change interval argument type to its representation
        new_arguments[1].type = std::make_shared<DataTypeNumber<DataTypeInterval::FieldType>>();

        auto function = function_builder->build(new_arguments);

        return function->execute(new_arguments, result_type, input_rows_count);
    }

    ColumnPtr executeTupleNumberOperator(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
                                               size_t input_rows_count, const FunctionOverloadResolverPtr & function_builder) const
    {
        ColumnsWithTypeAndName new_arguments = arguments;

        /// Number argument must be second.
        if (isNumber(arguments[0].type))
            std::swap(new_arguments[0], new_arguments[1]);

        auto function = function_builder->build(new_arguments);

        return function->execute(new_arguments, result_type, input_rows_count);
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
            const NativeResultType const_a = helperGetOrConvert<T0, ResultDataType>(col_left_const, left);
            const NativeResultType const_b = helperGetOrConvert<T1, ResultDataType>(col_right_const, right);

            ResultType res = {};
            if (!right_nullmap || !(*right_nullmap)[0])
                res = check_decimal_overflow
                    ? OpImplCheck::template process<left_is_decimal, right_is_decimal>(const_a, const_b, scale_a, scale_b)
                    : OpImpl::template process<left_is_decimal, right_is_decimal>(const_a, const_b, scale_a, scale_b);

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
            const NativeResultType const_a = helperGetOrConvert<T0, ResultDataType>(col_left_const, left);

            helperInvokeEither<OpCase::LeftConstant, left_is_decimal, right_is_decimal, OpImpl, OpImplCheck>(
                const_a, col_right->getData(), vec_res, scale_a, scale_b, right_nullmap);
        }
        else if (col_left && col_right_const)
        {
            const NativeResultType const_b = helperGetOrConvert<T1, ResultDataType>(col_right_const, right);

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
        return !division_by_nullable;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return ((IsOperation<Op>::div_int || IsOperation<Op>::modulo) && !arguments[1].is_const)
            || (IsOperation<Op>::div_floating && (isDecimalOrNullableDecimal(arguments[0].type) || isDecimalOrNullableDecimal(arguments[1].type)));
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
                throw Exception("Cannot add aggregate states of different functions: "
                    + arguments[0]->getName() + " and " + arguments[1]->getName(), ErrorCodes::CANNOT_ADD_DIFFERENT_AGGREGATE_STATES);

            return arguments[0];
        }

        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        if (auto function_builder = getFunctionForIntervalArithmetic(arguments[0], arguments[1], context))
        {
            ColumnsWithTypeAndName new_arguments(2);

            for (size_t i = 0; i < 2; ++i)
                new_arguments[i].type = arguments[i];

            /// Interval argument must be second.
            if (isDate(new_arguments[1].type) || isDateTime(new_arguments[1].type) || isDateTime64(new_arguments[1].type))
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
                            type_res = std::make_shared<LeftDataType>(left.getN());
                            return true;
                        }
                    }
                }

                if constexpr (!Op<LeftDataType, RightDataType>::allow_string_integer)
                    return false;
                else if constexpr (!IsIntegral<RightDataType>)
                    return false;
                else if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType>)
                    type_res = std::make_shared<LeftDataType>(left.getN());
                else
                    type_res = std::make_shared<DataTypeString>();
                return true;
            }
            else
            {
                using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

                if constexpr (!std::is_same_v<ResultDataType, InvalidType>)
                {
                    if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                    {
                        if constexpr (is_division)
                        {
                            if (context->getSettingsRef().decimal_check_overflow)
                            {
                                /// Check overflow by using operands scale (based on big decimal division implementation details):
                                /// big decimal arithmetic is based on big integers, decimal operands are converted to big integers
                                /// i.e. int_operand = decimal_operand*10^scale
                                /// For division, left operand will be scaled by right operand scale also to do big integer division,
                                /// BigInt result = left*10^(left_scale + right_scale) / right * 10^right_scale
                                /// So, we can check upfront possible overflow just by checking max scale used for left operand
                                /// Note: it doesn't detect all possible overflow during big decimal division
                                if (left.getScale() + right.getScale() > ResultDataType::maxPrecision())
                                    throw Exception("Overflow during decimal division", ErrorCodes::DECIMAL_OVERFLOW);
                            }
                        }
                        ResultDataType result_type = decimalResultType<is_multiply, is_division>(left, right);
                        type_res = std::make_shared<ResultDataType>(result_type.getPrecision(), result_type.getScale());
                    }
                    else if constexpr ((IsDataTypeDecimal<LeftDataType> && IsFloatingPoint<RightDataType>) ||
                        (IsDataTypeDecimal<RightDataType> && IsFloatingPoint<LeftDataType>))
                        type_res = std::make_shared<DataTypeFloat64>();
                    else if constexpr (IsDataTypeDecimal<LeftDataType>)
                        type_res = std::make_shared<LeftDataType>(left.getPrecision(), left.getScale());
                    else if constexpr (IsDataTypeDecimal<RightDataType>)
                        type_res = std::make_shared<RightDataType>(right.getPrecision(), right.getScale());
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
                    else
                        type_res = std::make_shared<ResultDataType>();
                    return true;
                }
            }
            return false;
        });

        if (valid)
            return type_res;

        throw Exception(
            "Illegal types " + arguments[0]->getName() +
            " and " + arguments[1]->getName() +
            " of arguments of function " + String(name),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    ColumnPtr executeFixedString(const ColumnsWithTypeAndName & arguments) const
    {
        using OpImpl = FixedStringOperationImpl<Op<UInt8, UInt8>>;

        const auto * const col_left_raw = arguments[0].column.get();
        const auto * const col_right_raw = arguments[1].column.get();

        if (const auto * col_left_const = checkAndGetColumnConst<ColumnFixedString>(col_left_raw))
        {
            if (const auto * col_right_const = checkAndGetColumnConst<ColumnFixedString>(col_right_raw))
            {
                const auto * col_left = checkAndGetColumn<ColumnFixedString>(col_left_const->getDataColumn());
                const auto * col_right = checkAndGetColumn<ColumnFixedString>(col_right_const->getDataColumn());

                if (col_left->getN() != col_right->getN())
                    return nullptr;

                auto col_res = ColumnFixedString::create(col_left->getN());
                auto & out_chars = col_res->getChars();

                out_chars.resize(col_left->getN());

                OpImpl::template process<OpCase::Vector>(
                    col_left->getChars().data(),
                    col_right->getChars().data(),
                    out_chars.data(),
                    out_chars.size(), {});

                return ColumnConst::create(std::move(col_res), col_left_raw->size());
            }
        }

        const bool is_left_column_const = checkAndGetColumnConst<ColumnFixedString>(col_left_raw) != nullptr;
        const bool is_right_column_const = checkAndGetColumnConst<ColumnFixedString>(col_right_raw) != nullptr;

        const auto * col_left = is_left_column_const
                        ? checkAndGetColumn<ColumnFixedString>(
                            checkAndGetColumnConst<ColumnFixedString>(col_left_raw)->getDataColumn())
                        : checkAndGetColumn<ColumnFixedString>(col_left_raw);
        const auto * col_right = is_right_column_const
                        ? checkAndGetColumn<ColumnFixedString>(
                            checkAndGetColumnConst<ColumnFixedString>(col_right_raw)->getDataColumn())
                        : checkAndGetColumn<ColumnFixedString>(col_right_raw);

        if (col_left && col_right)
        {
            if (col_left->getN() != col_right->getN())
                return nullptr;

            auto col_res = ColumnFixedString::create(col_left->getN());
            auto & out_chars = col_res->getChars();
            out_chars.resize((is_right_column_const ? col_left->size() : col_right->size()) * col_left->getN());

            if (!is_left_column_const && !is_right_column_const)
            {
                OpImpl::template process<OpCase::Vector>(
                    col_left->getChars().data(),
                    col_right->getChars().data(),
                    out_chars.data(),
                    out_chars.size(), {});
            }
            else if (is_left_column_const)
            {
                OpImpl::template process<OpCase::LeftConstant>(
                    col_left->getChars().data(),
                    col_right->getChars().data(),
                    out_chars.data(),
                    out_chars.size(),
                    col_left->getN());
            }
            else
            {
                OpImpl::template process<OpCase::RightConstant>(
                    col_left->getChars().data(),
                    col_right->getChars().data(),
                    out_chars.data(),
                    out_chars.size(),
                    col_left->getN());
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

        const auto * col_left = col_left_const ? checkAndGetColumn<LeftColumnType>(col_left_const->getDataColumn())
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

            return ColumnConst::create(std::move(col_res), col_left->size());
        }
        else if (!col_left_const && !col_right_const && col_right)
        {
            if constexpr (std::is_same_v<LeftDataType, DataTypeFixedString>)
            {
                OpImpl::template processFixedString<OpCase::Vector>(in_vec.data(), col_left->getN(), col_right->getData().data(), out_vec, col_left->size());
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
                OpImpl::template processFixedString<OpCase::RightConstant>(in_vec.data(), col_left->getN(), &value, out_vec, col_left->size());
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

        if constexpr (std::is_same_v<ResultDataType, InvalidType>)
            return nullptr;
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

            /// When Decimal op Float32/64, convert both of them into Float64
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

        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        if (auto function_builder = getFunctionForIntervalArithmetic(arguments[0].type, arguments[1].type, context))
        {
            return executeDateTimeIntervalPlusMinus(arguments, result_type, input_rows_count, function_builder);
        }

        /// Special case when the function is plus, minus or multiply, both arguments are tuples.
        if (auto function_builder = getFunctionForTupleArithmetic(arguments[0].type, arguments[1].type, context))
        {
            return function_builder->build(arguments)->execute(arguments, result_type, input_rows_count);
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
                                                              : checkAndGetColumn<ColumnNullable>(*right_argument.column);

            const auto & null_bytemap = nullable_column->getNullMapData();
            auto res = executeImpl2(createBlockWithNestedColumns(arguments), removeNullable(result_type), input_rows_count, &null_bytemap);
            return wrapInNullable(res, arguments, result_type, input_rows_count);
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

                if constexpr (!Op<LeftDataType, RightDataType>::allow_string_integer)
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
            else
                return (res = executeNumeric(arguments, left, right, right_nullmap)) != nullptr;
        });

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
    bool isCompilableImpl(const DataTypes & arguments) const override
    {
        if (2 != arguments.size())
            return false;

        return castBothTypes(arguments[0].get(), arguments[1].get(), [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType> || std::is_same_v<DataTypeFixedString, RightDataType> || std::is_same_v<DataTypeString, LeftDataType> || std::is_same_v<DataTypeString, RightDataType>)
                return false;
            else
            {
                using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;
                using OpSpec = Op<typename LeftDataType::FieldType, typename RightDataType::FieldType>;
                return !std::is_same_v<ResultDataType, InvalidType> && !IsDataTypeDecimal<ResultDataType> && OpSpec::compilable;
            }
        });
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, Values values) const override
    {
        assert(2 == types.size() && 2 == values.size());

        llvm::Value * result = nullptr;
        castBothTypes(types[0].get(), types[1].get(), [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            if constexpr (!std::is_same_v<DataTypeFixedString, LeftDataType> && !std::is_same_v<DataTypeFixedString, RightDataType> && !std::is_same_v<DataTypeString, LeftDataType> && !std::is_same_v<DataTypeString, RightDataType>)
            {
                using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;
                using OpSpec = Op<typename LeftDataType::FieldType, typename RightDataType::FieldType>;
                if constexpr (!std::is_same_v<ResultDataType, InvalidType> && !IsDataTypeDecimal<ResultDataType> && OpSpec::compilable)
                {
                    auto & b = static_cast<llvm::IRBuilder<> &>(builder);
                    auto type = std::make_shared<ResultDataType>();
                    auto * lval = nativeCast(b, types[0], values[0], type);
                    auto * rval = nativeCast(b, types[1], values[1], type);
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
        else if (right.column && isColumnConst(*right.column) && arguments.size() == 1)
        {
            ColumnsWithTypeAndName columns_with_constant
                = {arguments[0],
                   {right.column->cloneResized(input_rows_count), right.type, right.name}};

            return Base::executeImpl(columns_with_constant, result_type, input_rows_count);
        }
        else
            return Base::executeImpl(arguments, result_type, input_rows_count);
    }

    bool hasInformationAboutMonotonicity() const override
    {
        const std::string_view name_view = Name::name;
        return (name_view == "minus" || name_view == "plus" || name_view == "divide" || name_view == "intDiv");
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field & left_point, const Field & right_point) const override
    {
        const std::string_view name_view = Name::name;

        // For simplicity, we treat null values as monotonicity breakers, except for variable / non-zero constant.
        if (left_point.isNull() || right_point.isNull())
        {
            if (name_view == "divide" || name_view == "intDiv")
            {
                // variable / constant
                if (right.column && isColumnConst(*right.column))
                {
                    auto constant = (*right.column)[0];
                    if (applyVisitor(FieldVisitorAccurateEquals(), constant, Field(0)))
                        return {false, true, false}; // variable / 0 is undefined, let's treat it as non-monotonic
                    bool is_constant_positive = applyVisitor(FieldVisitorAccurateLess(), Field(0), constant);

                    // division is saturated to `inf`, thus it doesn't have overflow issues.
                    return {true, is_constant_positive, true};
                }
            }
            return {false, true, false};
        }

        // For simplicity, we treat every single value interval as positive monotonic.
        if (applyVisitor(FieldVisitorAccurateEquals(), left_point, right_point))
            return {true, true, false};

        if (name_view == "minus" || name_view == "plus")
        {
            // const +|- variable
            if (left.column && isColumnConst(*left.column))
            {
                auto transform = [&](const Field & point)
                {
                    ColumnsWithTypeAndName columns_with_constant
                        = {{left.column->cloneResized(1), left.type, left.name},
                           {right.type->createColumnConst(1, point), right.type, right.name}};

                    auto col = Base::executeImpl(columns_with_constant, return_type, 1);
                    Field point_transformed;
                    col->get(0, point_transformed);
                    return point_transformed;
                };
                transform(left_point);
                transform(right_point);

                if (name_view == "plus")
                {
                    // Check if there is an overflow
                    if (applyVisitor(FieldVisitorAccurateLess(), left_point, right_point)
                            == applyVisitor(FieldVisitorAccurateLess(), transform(left_point), transform(right_point)))
                        return {true, true, false};
                    else
                        return {false, true, false};
                }
                else
                {
                    // Check if there is an overflow
                    if (applyVisitor(FieldVisitorAccurateLess(), left_point, right_point)
                            != applyVisitor(FieldVisitorAccurateLess(), transform(left_point), transform(right_point)))
                        return {true, false, false};
                    else
                        return {false, false, false};
                }
            }
            // variable +|- constant
            else if (right.column && isColumnConst(*right.column))
            {
                auto transform = [&](const Field & point)
                {
                    ColumnsWithTypeAndName columns_with_constant
                        = {{left.type->createColumnConst(1, point), left.type, left.name},
                           {right.column->cloneResized(1), right.type, right.name}};

                    auto col = Base::executeImpl(columns_with_constant, return_type, 1);
                    Field point_transformed;
                    col->get(0, point_transformed);
                    return point_transformed;
                };

                // Check if there is an overflow
                if (applyVisitor(FieldVisitorAccurateLess(), left_point, right_point)
                    == applyVisitor(FieldVisitorAccurateLess(), transform(left_point), transform(right_point)))
                    return {true, true, false};
                else
                    return {false, true, false};
            }
        }
        if (name_view == "divide" || name_view == "intDiv")
        {
            // const / variable
            if (left.column && isColumnConst(*left.column))
            {
                auto constant = (*left.column)[0];
                if (applyVisitor(FieldVisitorAccurateEquals(), constant, Field(0)))
                    return {true, true, false}; // 0 / 0 is undefined, thus it's not always monotonic

                bool is_constant_positive = applyVisitor(FieldVisitorAccurateLess(), Field(0), constant);
                if (applyVisitor(FieldVisitorAccurateLess(), left_point, Field(0))
                    && applyVisitor(FieldVisitorAccurateLess(), right_point, Field(0)))
                {
                    return {true, is_constant_positive, false};
                }
                else if (
                    applyVisitor(FieldVisitorAccurateLess(), Field(0), left_point)
                    && applyVisitor(FieldVisitorAccurateLess(), Field(0), right_point))
                {
                    return {true, !is_constant_positive, false};
                }
            }
            // variable / constant
            else if (right.column && isColumnConst(*right.column))
            {
                auto constant = (*right.column)[0];
                if (applyVisitor(FieldVisitorAccurateEquals(), constant, Field(0)))
                    return {false, true, false}; // variable / 0 is undefined, let's treat it as non-monotonic

                bool is_constant_positive = applyVisitor(FieldVisitorAccurateLess(), Field(0), constant);
                // division is saturated to `inf`, thus it doesn't have overflow issues.
                return {true, is_constant_positive, true};
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
        /// Check the case when operation is divide, intDiv or modulo and denominator is Nullable(Something).
        /// For divide operation we should check only Nullable(Decimal), because only this case can throw division by zero error.
        bool division_by_nullable = !arguments[0].type->onlyNull() && !arguments[1].type->onlyNull() && arguments[1].type->isNullable()
            && (IsOperation<Op>::div_int || IsOperation<Op>::modulo
                || (IsOperation<Op>::div_floating
                    && (isDecimalOrNullableDecimal(arguments[0].type) || isDecimalOrNullableDecimal(arguments[1].type))));

        /// More efficient specialization for two numeric arguments.
        if (arguments.size() == 2
            && ((arguments[0].column && isColumnConst(*arguments[0].column))
                || (arguments[1].column && isColumnConst(*arguments[1].column))))
        {
            auto function = division_by_nullable ? FunctionBinaryArithmeticWithConstants<Op, Name, valid_on_default_arguments, valid_on_float_arguments, true>::create(
                    arguments[0], arguments[1], return_type, context)
                : FunctionBinaryArithmeticWithConstants<Op, Name, valid_on_default_arguments, valid_on_float_arguments, false>::create(
                    arguments[0], arguments[1], return_type, context);

            return std::make_unique<FunctionToFunctionBaseAdaptor>(
                function,
                collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
        }
        auto function = division_by_nullable
            ? FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments, valid_on_float_arguments, true>::create(context)
            : FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments, valid_on_float_arguments, false>::create(context);

        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            function,
            collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
            return_type);

    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size()) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        return FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments, valid_on_float_arguments>::getReturnTypeImplStatic(arguments, context);
    }

private:
    ContextPtr context;
};
}
