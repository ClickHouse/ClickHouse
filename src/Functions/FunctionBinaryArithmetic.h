#pragma once

// Include this first, because `#define _asan_poison_address` from
// llvm/Support/Compiler.h conflicts with its forward declaration in
// sanitizer/asan_interface.h
#include <memory>
#include <type_traits>
#include <common/wide_integer_to_string.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/Native.h>
#include <DataTypes/NumberTraits.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnAggregateFunction.h>
#include "Core/DecimalFunctions.h"
#include "IFunction.h"
#include "FunctionHelpers.h"
#include "IsOperation.h"
#include "DivisionUtils.h"
#include "castTypeToEither.h"
#include "FunctionFactory.h"
#include <Common/Arena.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <common/map.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

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

        /// e.g Decimal * Float64 = Float64
        Case<IsOperation<Operation>::multiply && IsDataTypeDecimal<LeftDataType> && IsFloatingPoint<RightDataType>,
            RightDataType>,
        Case<IsOperation<Operation>::multiply && IsDataTypeDecimal<RightDataType> && IsFloatingPoint<LeftDataType>,
            LeftDataType>,

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

template <class T>
inline constexpr const auto & undec(const T & x)
{
    if constexpr (IsDecimalNumber<T>)
        return x.value;
    else
        return x;
}

template <typename A, typename B, typename Op, typename OpResultType = typename Op::ResultType>
struct BinaryOperation
{
    using ResultType = OpResultType;
    static const constexpr bool allow_fixed_string = false;

    template <OpCase op_case>
    static void NO_INLINE process(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
            if constexpr (op_case == OpCase::Vector)
                c[i] = Op::template apply<ResultType>(a[i], b[i]);
            else if constexpr (op_case == OpCase::LeftConstant)
                c[i] = Op::template apply<ResultType>(*a, b[i]);
            else
                c[i] = Op::template apply<ResultType>(a[i], *b);
    }

    static ResultType process(A a, B b) { return Op::template apply<ResultType>(a, b); }
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
    using NativeResultType = typename NativeType<ResultType>::Type; // e.g. UInt32 for Decimal32

    using ResultContainerType = typename std::conditional_t<IsDecimalNumber<ResultType>,
        ColumnDecimal<ResultType>,
        ColumnVector<ResultType>>::Container;

public:
    template <OpCase op_case, bool is_decimal_a, bool is_decimal_b, class A, class B>
    static void NO_INLINE process(const A & a, const B & b, ResultContainerType & c,
        NativeResultType scale_a, NativeResultType scale_b)
    {
        if constexpr (op_case == OpCase::LeftConstant) static_assert(!IsDecimalNumber<A>);
        if constexpr (op_case == OpCase::RightConstant) static_assert(!IsDecimalNumber<B>);

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
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledDiv<is_decimal_a>(
                    unwrap<op_case, OpCase::LeftConstant>(a, i),
                    unwrap<op_case, OpCase::RightConstant>(b, i),
                    scale_a);
            return;
        }

        for (size_t i = 0; i < size; ++i)
            c[i] = apply(
                unwrap<op_case, OpCase::LeftConstant>(a, i),
                unwrap<op_case, OpCase::RightConstant>(b, i));
    }

    template <bool is_decimal_a, bool is_decimal_b, class A, class B>
    static ResultType process(A a, B b, NativeResultType scale_a, NativeResultType scale_b)
    {
        static_assert(!IsDecimalNumber<A>);
        static_assert(!IsDecimalNumber<B>);

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

template <template <typename, typename> class Op, typename Name, bool valid_on_default_arguments = true, bool valid_on_float_arguments = true>
class FunctionBinaryArithmetic : public IFunction
{
    static constexpr const bool is_plus = IsOperation<Op>::plus;
    static constexpr const bool is_minus = IsOperation<Op>::minus;
    static constexpr const bool is_multiply = IsOperation<Op>::multiply;
    static constexpr const bool is_division = IsOperation<Op>::division;

    ContextPtr context;
    bool check_decimal_overflow = true;

    template <typename F>
    static bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64,
            DataTypeUInt128,
            DataTypeUInt256,
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeInt128,
            DataTypeInt256,
            DataTypeFloat32,
            DataTypeFloat64,
            DataTypeDate,
            DataTypeDateTime,
            DataTypeDecimal<Decimal32>,
            DataTypeDecimal<Decimal64>,
            DataTypeDecimal<Decimal128>,
            DataTypeDecimal<Decimal256>,
            DataTypeFixedString
        >(type, std::forward<F>(f));
    }

    template <typename F>
    static bool castTypeNoFloats(const IDataType * type, F && f)
    {
        return castTypeToEither<
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64,
            DataTypeUInt128,
            DataTypeUInt256,
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeInt128,
            DataTypeInt256,
            DataTypeDate,
            DataTypeDateTime,
            DataTypeDecimal<Decimal32>,
            DataTypeDecimal<Decimal64>,
            DataTypeDecimal<Decimal128>,
            DataTypeDecimal<Decimal256>,
            DataTypeFixedString
        >(type, std::forward<F>(f));
    }

    template <typename F>
    static bool castBothTypes(const IDataType * left, const IDataType * right, F && f)
    {
        if constexpr (valid_on_float_arguments)
        {
            return castType(left, [&](const auto & left_)
            {
                return castType(right, [&](const auto & right_)
                {
                    return f(left_, right_);
                });
            });
        }
        else
        {
            return castTypeNoFloats(left, [&](const auto & left_)
            {
                return castTypeNoFloats(right, [&](const auto & right_)
                {
                    return f(left_, right_);
                });
            });
        }
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
            throw Exception("Wrong order of arguments for function " + String(name) + ": argument of type Interval cannot be first.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        std::string function_name;
        if (interval_data_type)
        {
            function_name = String(is_plus ? "add" : "subtract") + interval_data_type->getKind().toString() + 's';
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

    template <typename T, typename ResultDataType, typename CC, typename C>
    static auto helperGetOrConvert(const CC & col_const, const C & col)
    {
        using ResultType = typename ResultDataType::FieldType;
        using NativeResultType = typename NativeType<ResultType>::Type;

        if constexpr (IsFloatingPoint<ResultDataType> && IsDecimalNumber<T>)
            return DecimalUtils::convertTo<NativeResultType>(col_const->template getValue<T>(), col.getScale());
        else if constexpr (IsDecimalNumber<T>)
            return col_const->template getValue<T>().value;
        else
            return col_const->template getValue<T>();
    }

    template <OpCase op_case, bool left_decimal, bool right_decimal, typename OpImpl, typename OpImplCheck,
              typename L, typename R, typename VR, typename SA, typename SB>
    void helperInvokeEither(const L& left, const R& right, VR& vec_res, SA scale_a, SB scale_b) const
    {
        if (check_decimal_overflow)
            OpImplCheck::template process<op_case, left_decimal, right_decimal>(left, right, vec_res, scale_a, scale_b);
        else
            OpImpl::template process<op_case, left_decimal, right_decimal>(left, right, vec_res, scale_a, scale_b);
    }

    template <class LeftDataType, class RightDataType, class ResultDataType,
              class L, class R, class CL, class CR>
    ColumnPtr executeNumericWithDecimal(
        const L & left, const R & right,
        const ColumnConst * const col_left_const, const ColumnConst * const col_right_const,
        const CL * const col_left, const CR * const col_right,
        size_t col_left_size) const
    {
        using T0 = typename LeftDataType::FieldType;
        using T1 = typename RightDataType::FieldType;
        using ResultType = typename ResultDataType::FieldType;

        using NativeResultType = typename NativeType<ResultType>::Type;
        using OpImpl = DecimalBinaryOperation<Op, ResultType, false>;
        using OpImplCheck = DecimalBinaryOperation<Op, ResultType, true>;

        using ColVecResult = std::conditional_t<IsDecimalNumber<ResultType>,
            ColumnDecimal<ResultType>, ColumnVector<ResultType>>;

        static constexpr const bool left_is_decimal = IsDecimalNumber<T0>;
        static constexpr const bool right_is_decimal = IsDecimalNumber<T1>;
        static constexpr const bool result_is_decimal = IsDataTypeDecimal<ResultDataType>;

        typename ColVecResult::MutablePtr col_res = nullptr;

        const ResultDataType type = [&]
        {
            if constexpr (left_is_decimal && IsFloatingPoint<RightDataType>)
                return RightDataType();
            else if constexpr (right_is_decimal && IsFloatingPoint<LeftDataType>)
                return LeftDataType();
            else
                return decimalResultType<is_multiply, is_division>(left, right);
        }();

        const ResultType scale_a = [&]
        {
            if constexpr (IsDataTypeDecimal<RightDataType> && is_division)
                return right.getScaleMultiplier(); // the division impl uses only the scale_a
            else if constexpr (result_is_decimal)
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
            else if constexpr (left_is_decimal)
            {
                if (col_left_const)
                    // the column will be converted to native type later, no need to scale it twice.
                    // the explicit type is needed to specify lambda return type
                    return ResultType{1};

                return 1 / DecimalUtils::convertTo<ResultType>(left.getScaleMultiplier(), 0);
            }
            else
                return 1; // the default value which won't cause any re-scale
        }();

        const ResultType scale_b = [&]
        {
            if constexpr (result_is_decimal)
            {
                if constexpr (is_multiply)
                    return ResultType{1};
                else
                    return type.scaleFactorFor(right, is_division);
            }
            else if constexpr (right_is_decimal)
            {
                if (col_right_const)
                    return ResultType{1};

                return 1 / DecimalUtils::convertTo<ResultType>(right.getScaleMultiplier(), 0);
            }
            else
                return 1;
        }();

        /// non-vector result
        if (col_left_const && col_right_const)
        {
            const NativeResultType const_a = helperGetOrConvert<T0, ResultDataType>(col_left_const, left);
            const NativeResultType const_b = helperGetOrConvert<T1, ResultDataType>(col_right_const, right);

            const ResultType res = check_decimal_overflow
                ? OpImplCheck::template process<left_is_decimal, right_is_decimal>(const_a, const_b, scale_a, scale_b)
                : OpImpl::template process<left_is_decimal, right_is_decimal>(const_a, const_b, scale_a, scale_b);

            if constexpr (result_is_decimal)
                return ResultDataType(type.getPrecision(), type.getScale()).createColumnConst(
                    col_left_const->size(), toField(res, type.getScale()));
            else
                 return ResultDataType().createColumnConst(col_left_const->size(), toField(res));
        }

        if constexpr (result_is_decimal)
            col_res = ColVecResult::create(0, type.getScale());
        else
            col_res = ColVecResult::create(0);

        auto & vec_res = col_res->getData();
        vec_res.resize(col_left_size);

        if (col_left && col_right)
        {
            helperInvokeEither<OpCase::Vector, left_is_decimal, right_is_decimal, OpImpl, OpImplCheck>(
                col_left->getData(), col_right->getData(), vec_res, scale_a, scale_b);
        }
        else if (col_left_const && col_right)
        {
            const NativeResultType const_a = helperGetOrConvert<T0, ResultDataType>(col_left_const, left);

            helperInvokeEither<OpCase::LeftConstant, left_is_decimal, right_is_decimal, OpImpl, OpImplCheck>(
                const_a, col_right->getData(), vec_res, scale_a, scale_b);
        }
        else if (col_left && col_right_const)
        {
            const NativeResultType const_b = helperGetOrConvert<T1, ResultDataType>(col_right_const, right);

            helperInvokeEither<OpCase::RightConstant, left_is_decimal, right_is_decimal, OpImpl, OpImplCheck>(
                col_left->getData(), const_b, vec_res, scale_a, scale_b);
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

        DataTypePtr type_res;

        const bool valid = castBothTypes(arguments[0].get(), arguments[1].get(), [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;

            if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType> ||
                          std::is_same_v<DataTypeFixedString, RightDataType>)
            {
                if constexpr (!Op<DataTypeFixedString, DataTypeFixedString>::allow_fixed_string)
                    return false;
                else if constexpr (std::is_same_v<LeftDataType, RightDataType>)
                {
                    if (left.getN() == right.getN())
                    {
                        type_res = std::make_shared<LeftDataType>(left.getN());
                        return true;
                    }
                }
            }
            else
            {
                using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

                if constexpr (!std::is_same_v<ResultDataType, InvalidType>)
                {
                    if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                    {
                        ResultDataType result_type = decimalResultType<is_multiply, is_division>(left, right);
                        type_res = std::make_shared<ResultDataType>(result_type.getPrecision(), result_type.getScale());
                    }
                    else if constexpr ((IsDataTypeDecimal<LeftDataType> && IsFloatingPoint<RightDataType>) ||
                        (IsDataTypeDecimal<RightDataType> && IsFloatingPoint<LeftDataType>))
                        type_res = std::make_shared<std::conditional_t<IsFloatingPoint<LeftDataType>,
                            LeftDataType, RightDataType>>();
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

    template <typename A, typename B>
    ColumnPtr executeNumeric(const ColumnsWithTypeAndName & arguments, const A & left, const B & right) const
    {
        using LeftDataType = std::decay_t<decltype(left)>;
        using RightDataType = std::decay_t<decltype(right)>;
        using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

        if constexpr (std::is_same_v<ResultDataType, InvalidType>)
            return nullptr;
        else // we can't avoid the else because otherwise the compiler may assume the ResultDataType may be Invalid
             // and that would produce the compile error.
        {
            using T0 = typename LeftDataType::FieldType;
            using T1 = typename RightDataType::FieldType;
            using ResultType = typename ResultDataType::FieldType;
            using ColVecT0 = std::conditional_t<IsDecimalNumber<T0>, ColumnDecimal<T0>, ColumnVector<T0>>;
            using ColVecT1 = std::conditional_t<IsDecimalNumber<T1>, ColumnDecimal<T1>, ColumnVector<T1>>;
            using ColVecResult = std::conditional_t<IsDecimalNumber<ResultType>, ColumnDecimal<ResultType>, ColumnVector<ResultType>>;

            const auto * const col_left_raw = arguments[0].column.get();
            const auto * const col_right_raw = arguments[1].column.get();

            const size_t col_left_size = col_left_raw->size();

            const ColumnConst * const col_left_const = checkAndGetColumnConst<ColVecT0>(col_left_raw);
            const ColumnConst * const col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw);

            const ColVecT0 * const col_left = checkAndGetColumn<ColVecT0>(col_left_raw);
            const ColVecT1 * const col_right = checkAndGetColumn<ColVecT1>(col_right_raw);

            if constexpr (IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>)
            {
                return executeNumericWithDecimal<LeftDataType, RightDataType, ResultDataType>(
                    left, right,
                    col_left_const, col_right_const,
                    col_left, col_right,
                    col_left_size);
            }
            else // can't avoid else and another indentation level, otherwise the compiler would try to instantiate
                 // ColVecResult for Decimals which would lead to a compile error.
            {
                using OpImpl = BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>;

                /// non-vector result
                if (col_left_const && col_right_const)
                {
                    const auto res = OpImpl::process(
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
                        vec_res.size());
                }
                else if (col_left_const && col_right)
                {
                    const T0 value = col_left_const->template getValue<T0>();

                    OpImpl::template process<OpCase::LeftConstant>(
                        &value,
                        col_right->getData().data(),
                        vec_res.data(),
                        vec_res.size());
                }
                else if (col_left && col_right_const)
                {
                    const T1 value = col_right_const->template getValue<T1>();

                    OpImpl::template process<OpCase::RightConstant>(
                        col_left->getData().data(),
                        &value,
                        vec_res.data(),
                        vec_res.size());
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
        if (auto function_builder
            = getFunctionForIntervalArithmetic(arguments[0].type, arguments[1].type, context))
        {
            return executeDateTimeIntervalPlusMinus(arguments, result_type, input_rows_count, function_builder);
        }

        const auto & left_argument = arguments[0];
        const auto & right_argument = arguments[1];
        const auto * const left_generic = left_argument.type.get();
        const auto * const right_generic = right_argument.type.get();
        ColumnPtr res;

        const bool valid = castBothTypes(left_generic, right_generic, [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;

            if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType> ||
                std::is_same_v<DataTypeFixedString, RightDataType>)
            {
                if constexpr (!Op<DataTypeFixedString, DataTypeFixedString>::allow_fixed_string)
                    return false;
                else
                    return (res = executeFixedString(arguments)) != nullptr;
            }
            else
                return (res = executeNumeric(arguments, left, right)) != nullptr;
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
            if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType> || std::is_same_v<DataTypeFixedString, RightDataType>)
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
            if constexpr (!std::is_same_v<DataTypeFixedString, LeftDataType> && !std::is_same_v<DataTypeFixedString, RightDataType>)
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


template <template <typename, typename> class Op, typename Name, bool valid_on_default_arguments = true, bool valid_on_float_arguments = true>
class FunctionBinaryArithmeticWithConstants : public FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments, valid_on_float_arguments>
{
public:
    using Base = FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments, valid_on_float_arguments>;
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
        // For simplicity, we treat null values as monotonicity breakers.
        if (left_point.isNull() || right_point.isNull())
            return {false, true, false};

        // For simplicity, we treat every single value interval as positive monotonic.
        if (applyVisitor(FieldVisitorAccurateEquals(), left_point, right_point))
            return {true, true, false};

        const std::string_view name_view = Name::name;

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
                if (applyVisitor(FieldVisitorAccurateLess(), left_point, Field(0)) &&
                        applyVisitor(FieldVisitorAccurateLess(), right_point, Field(0)))
                {
                    return {true, is_constant_positive, false};
                }
                else
                if (applyVisitor(FieldVisitorAccurateLess(), Field(0), left_point) &&
                        applyVisitor(FieldVisitorAccurateLess(), Field(0), right_point))
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
                return {true, is_constant_positive, false};
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
        /// More efficient specialization for two numeric arguments.
        if (arguments.size() == 2
            && ((arguments[0].column && isColumnConst(*arguments[0].column))
                || (arguments[1].column && isColumnConst(*arguments[1].column))))
        {
            return std::make_unique<FunctionToFunctionBaseAdaptor>(
                FunctionBinaryArithmeticWithConstants<Op, Name, valid_on_default_arguments, valid_on_float_arguments>::create(
                    arguments[0], arguments[1], return_type, context),
                collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
        }

        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments, valid_on_float_arguments>::create(context),
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
