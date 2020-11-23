#pragma once

// Include this first, because `#define _asan_poison_address` from
// llvm/Support/Compiler.h conflicts with its forward declaration in
// sanitizer/asan_interface.h
#include <Common/Arena.h>

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
#include "IFunctionImpl.h"
#include "FunctionHelpers.h"
#include "IsOperation.h"
#include "DivisionUtils.h"
#include "castTypeToEither.h"
#include "FunctionFactory.h"
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitors.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <ext/map.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_EMBEDDED_COMPILER
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wunused-parameter"
#    include <llvm/IR/IRBuilder.h>
#    pragma GCC diagnostic pop
#endif


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


/** Arithmetic operations: +, -, *, /, %,
  * intDiv (integer division)
  * Bitwise operations: |, &, ^, ~.
  * Etc.
  */

template <typename A, typename B, typename Op, typename ResultType_ = typename Op::ResultType>
struct BinaryOperation
{
    using ResultType = ResultType_;
    static const constexpr bool allow_fixed_string = false;

    static void NO_INLINE vectorVector(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::template apply<ResultType>(a[i], b[i]);
    }

    static void NO_INLINE vectorConstant(const A * __restrict a, B b, ResultType * __restrict c, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::template apply<ResultType>(a[i], b);
    }

    static void NO_INLINE constantVector(A a, const B * __restrict b, ResultType * __restrict c, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::template apply<ResultType>(a, b[i]);
    }

    static ResultType constantConstant(A a, B b)
    {
        return Op::template apply<ResultType>(a, b);
    }
};

template <typename Op>
struct FixedStringOperationImpl
{
    static void NO_INLINE vectorVector(const UInt8 * __restrict a, const UInt8 * __restrict b, UInt8 * __restrict c, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::template apply<UInt8>(a[i], b[i]);
    }

    template <bool inverted>
    static void NO_INLINE vector_constant_impl(const UInt8 * __restrict a, const UInt8 * __restrict b, UInt8 * __restrict c, size_t size, size_t N)
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
        size_t b_increment = 16 % N;

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

    static void vectorConstant(const UInt8 * __restrict a, const UInt8 * __restrict b, UInt8 * __restrict c, size_t size, size_t N)
    {
        vector_constant_impl<false>(a, b, c, size, N);
    }

    static void constantVector(const UInt8 * __restrict a, const UInt8 * __restrict b, UInt8 * __restrict c, size_t size, size_t N)
    {
        vector_constant_impl<true>(b, a, c, size, N);
    }
};


template <typename A, typename B, typename Op, typename ResultType = typename Op::ResultType>
struct BinaryOperationImpl : BinaryOperation<A, B, Op, ResultType>
{
};

template <typename T>
inline constexpr const auto & undec(const T & x)
{
    if constexpr (IsDecimalNumber<T>)
        return x.value;
    else
        return x;
}

/// Binary operations for Decimals need scale args
/// +|- scale one of args (which scale factor is not 1). ScaleR = oneof(Scale1, Scale2);
/// *   no agrs scale. ScaleR = Scale1 + Scale2;
/// /   first arg scale. ScaleR = Scale1 (scale_a = DecimalType<B>::getScale()).
template <template <typename, typename> typename Operation, typename ResultType_, bool check_overflow = true>
struct DecimalBinaryOperation
{
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

    using ResultType = ResultType_;
    using NativeResultType = typename NativeType<ResultType>::Type;
    using Op = std::conditional_t<is_float_division,
        DivideIntegralImpl<NativeResultType, NativeResultType>, /// substitute divide by intDiv (throw on division by zero)
        Operation<NativeResultType, NativeResultType>>;

    using ArrayC = typename ColumnDecimal<ResultType>::Container;

    template <bool is_decimal_a, bool is_decimal_b, typename ArrayA, typename ArrayB>
    static void NO_INLINE vectorVector(const ArrayA & a, const ArrayB & b, ArrayC & c,
                                       NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]])
    {
        size_t size = a.size();
        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(undec(a[i]), undec(b[i]), scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(undec(a[i]), undec(b[i]), scale_b);
                return;
            }
        }
        else if constexpr (is_division && is_decimal_b)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledDiv<is_decimal_a>(undec(a[i]), undec(b[i]), scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(undec(a[i]), undec(b[i]));
    }

    template <bool is_decimal_a, bool is_decimal_b, typename ArrayA, typename B>
    static void NO_INLINE vectorConstant(const ArrayA & a, B b, ArrayC & c,
                                         NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]])
    {
        static_assert(!IsDecimalNumber<B>);

        size_t size = a.size();
        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(undec(a[i]), b, scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(undec(a[i]), b, scale_b);
                return;
            }
        }
        else if constexpr (is_division && is_decimal_b)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledDiv<is_decimal_a>(undec(a[i]), b, scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(undec(a[i]), b);
    }

    template <bool is_decimal_a, bool is_decimal_b, typename A, typename ArrayB>
    static void NO_INLINE constantVector(A a, const ArrayB & b, ArrayC & c,
                                         NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]])
    {
        static_assert(!IsDecimalNumber<A>);

        size_t size = b.size();
        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a, undec(b[i]), scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a, undec(b[i]), scale_b);
                return;
            }
        }
        else if constexpr (is_division && is_decimal_b)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledDiv<is_decimal_a>(a, undec(b[i]), scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a, undec(b[i]));
    }

    template <bool is_decimal_a, bool is_decimal_b, typename A, typename B>
    static ResultType constantConstant(A a, B b, NativeResultType scale_a [[maybe_unused]], NativeResultType scale_b [[maybe_unused]])
    {
        static_assert(!IsDecimalNumber<A>);
        static_assert(!IsDecimalNumber<B>);

        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
                return applyScaled<true>(a, b, scale_a);
            else if (scale_b != 1)
                return applyScaled<false>(a, b, scale_b);
        }
        else if constexpr (is_division && is_decimal_b)
            return applyScaledDiv<is_decimal_a>(a, b, scale_a);
        return apply(a, b);
    }

private:
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

    template <bool scale_left>
    static NO_SANITIZE_UNDEFINED NativeResultType applyScaled(NativeResultType a, NativeResultType b, NativeResultType scale)
    {
        if constexpr (is_plus_minus_compare)
        {
            NativeResultType res;

            if constexpr (check_overflow)
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


/// Used to indicate undefined operation
struct InvalidType;

template <bool V, typename T> struct Case : std::bool_constant<V> { using type = T; };

/// Switch<Case<C0, T0>, ...> -- select the first Ti for which Ci is true; InvalidType if none.
template <typename... Ts> using Switch = typename std::disjunction<Ts..., Case<true, InvalidType>>::type;

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
template <> inline constexpr bool IsExtended<DataTypeUInt256> = true;
template <> inline constexpr bool IsExtended<DataTypeInt128> = true;
template <> inline constexpr bool IsExtended<DataTypeInt256> = true;

template <typename DataType> constexpr bool IsIntegralOrExtended = IsIntegral<DataType> || IsExtended<DataType>;
template <typename DataType> constexpr bool IsIntegralOrExtendedOrDecimal = IsIntegralOrExtended<DataType> || IsDataTypeDecimal<DataType>;

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

template <typename T> using DataTypeFromFieldType = std::conditional_t<std::is_same_v<T, NumberTraits::Error>, InvalidType, DataTypeNumber<T>>;

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


template <template <typename, typename> class Op, typename Name, bool valid_on_default_arguments = true>
class FunctionBinaryArithmetic : public IFunction
{
    static constexpr const bool is_plus = IsOperation<Op>::plus;
    static constexpr const bool is_minus = IsOperation<Op>::minus;
    static constexpr const bool is_multiply = IsOperation<Op>::multiply;
    static constexpr const bool is_division = IsOperation<Op>::division;

    const Context & context;
    bool check_decimal_overflow = true;

    template <typename F>
    static bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64,
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
    static bool castBothTypes(const IDataType * left, const IDataType * right, F && f)
    {
        return castType(left, [&](const auto & left_) { return castType(right, [&](const auto & right_) { return f(left_, right_); }); });
    }

    static FunctionOverloadResolverPtr
    getFunctionForIntervalArithmetic(const DataTypePtr & type0, const DataTypePtr & type1, const Context & context)
    {
        bool first_is_date_or_datetime = isDateOrDateTime(type0);
        bool second_is_date_or_datetime = isDateOrDateTime(type1);

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
        if (WhichDataType(arguments[1].type).isDateOrDateTime())
            std::swap(new_arguments[0], new_arguments[1]);

        /// Change interval argument type to its representation
        new_arguments[1].type = std::make_shared<DataTypeNumber<DataTypeInterval::FieldType>>();

        auto function = function_builder->build(new_arguments);

        return function->execute(new_arguments, result_type, input_rows_count);
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBinaryArithmetic>(context); }

    explicit FunctionBinaryArithmetic(const Context & context_)
    :   context(context_),
        check_decimal_overflow(decimalCheckArithmeticOverflow(context))
    {}

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return getReturnTypeImplStatic(arguments, context);
    }

    static DataTypePtr getReturnTypeImplStatic(const DataTypes & arguments, const Context & context)
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
            if (WhichDataType(new_arguments[1].type).isDateOrDateTime())
                std::swap(new_arguments[0], new_arguments[1]);

            /// Change interval argument to its representation
            new_arguments[1].type = std::make_shared<DataTypeNumber<DataTypeInterval::FieldType>>();

            auto function = function_builder->build(new_arguments);
            return function->getResultType();
        }

        DataTypePtr type_res;
        bool valid = castBothTypes(arguments[0].get(), arguments[1].get(), [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType> || std::is_same_v<DataTypeFixedString, RightDataType>)
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
        if (!valid)
            throw Exception("Illegal types " + arguments[0]->getName() + " and " + arguments[1]->getName() + " of arguments of function " + String(name),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return type_res;
    }

    ColumnPtr executeFixedString(const ColumnsWithTypeAndName & arguments) const
    {
        using OpImpl = FixedStringOperationImpl<Op<UInt8, UInt8>>;

        const auto * col_left_raw = arguments[0].column.get();
        const auto * col_right_raw = arguments[1].column.get();
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
                OpImpl::vectorVector(col_left->getChars().data(),
                                      col_right->getChars().data(),
                                      out_chars.data(),
                                      out_chars.size());
                return ColumnConst::create(std::move(col_res), col_left_raw->size());
            }
        }

        bool is_left_column_const = checkAndGetColumnConst<ColumnFixedString>(col_left_raw) != nullptr;
        bool is_right_column_const = checkAndGetColumnConst<ColumnFixedString>(col_right_raw) != nullptr;

        const auto * col_left = is_left_column_const
                        ? checkAndGetColumn<ColumnFixedString>(checkAndGetColumnConst<ColumnFixedString>(col_left_raw)->getDataColumn())
                        : checkAndGetColumn<ColumnFixedString>(col_left_raw);
        const auto * col_right = is_right_column_const
                        ? checkAndGetColumn<ColumnFixedString>(checkAndGetColumnConst<ColumnFixedString>(col_right_raw)->getDataColumn())
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
                OpImpl::vectorVector(
                    col_left->getChars().data(),
                    col_right->getChars().data(),
                    out_chars.data(),
                    out_chars.size());
            }
            else if (is_left_column_const)
            {
                OpImpl::constantVector(
                    col_left->getChars().data(),
                    col_right->getChars().data(),
                    out_chars.data(),
                    out_chars.size(),
                    col_left->getN());
            }
            else
            {
                OpImpl::vectorConstant(
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

        if constexpr (!std::is_same_v<ResultDataType, InvalidType>)
        {
            using T0 = typename LeftDataType::FieldType;
            using T1 = typename RightDataType::FieldType;
            using ResultType = typename ResultDataType::FieldType;
            using ColVecT0 = std::conditional_t<IsDecimalNumber<T0>, ColumnDecimal<T0>, ColumnVector<T0>>;
            using ColVecT1 = std::conditional_t<IsDecimalNumber<T1>, ColumnDecimal<T1>, ColumnVector<T1>>;
            using ColVecResult = std::conditional_t<IsDecimalNumber<ResultType>, ColumnDecimal<ResultType>, ColumnVector<ResultType>>;

            const auto * col_left_raw = arguments[0].column.get();
            const auto * col_right_raw = arguments[1].column.get();

            auto col_left_const = checkAndGetColumnConst<ColVecT0>(col_left_raw);
            auto col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw);

            typename ColVecResult::MutablePtr col_res = nullptr;

            auto col_left = checkAndGetColumn<ColVecT0>(col_left_raw);
            auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw);

            if constexpr (IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>)
            {
                using NativeResultType = typename NativeType<ResultType>::Type;
                using OpImpl = DecimalBinaryOperation<Op, ResultType, false>;
                using OpImplCheck = DecimalBinaryOperation<Op, ResultType, true>;

                ResultDataType type = decimalResultType<is_multiply, is_division>(left, right);

                static constexpr const bool dec_a = IsDecimalNumber<T0>;
                static constexpr const bool dec_b = IsDecimalNumber<T1>;

                typename ResultDataType::FieldType scale_a = type.scaleFactorFor(left, is_multiply);
                typename ResultDataType::FieldType scale_b = type.scaleFactorFor(right, is_multiply || is_division);
                if constexpr (IsDataTypeDecimal<RightDataType> && is_division)
                    scale_a = right.getScaleMultiplier();

                /// non-vector result
                if (col_left_const && col_right_const)
                {
                    NativeResultType const_a = col_left_const->template getValue<T0>();
                    NativeResultType const_b = col_right_const->template getValue<T1>();

                    auto res = check_decimal_overflow ?
                        OpImplCheck::template constantConstant<dec_a, dec_b>(const_a, const_b, scale_a, scale_b) :
                        OpImpl::template constantConstant<dec_a, dec_b>(const_a, const_b, scale_a, scale_b);

                    return ResultDataType(type.getPrecision(), type.getScale()).createColumnConst(
                            col_left_const->size(), toField(res, type.getScale()));
                }

                col_res = ColVecResult::create(0, type.getScale());
                auto & vec_res = col_res->getData();
                vec_res.resize(col_left_raw->size());

                if (col_left && col_right)
                {
                    if (check_decimal_overflow)
                        OpImplCheck::template vectorVector<dec_a, dec_b>(col_left->getData(), col_right->getData(), vec_res, scale_a, scale_b);
                    else
                        OpImpl::template vectorVector<dec_a, dec_b>(col_left->getData(), col_right->getData(), vec_res, scale_a, scale_b);
                }
                else if (col_left_const && col_right)
                {
                    NativeResultType const_a = col_left_const->template getValue<T0>();

                    if (check_decimal_overflow)
                        OpImplCheck::template constantVector<dec_a, dec_b>(const_a, col_right->getData(), vec_res, scale_a, scale_b);
                    else
                        OpImpl::template constantVector<dec_a, dec_b>(const_a, col_right->getData(), vec_res, scale_a, scale_b);
                }
                else if (col_left && col_right_const)
                {
                    NativeResultType const_b = col_right_const->template getValue<T1>();

                    if (check_decimal_overflow)
                        OpImplCheck::template vectorConstant<dec_a, dec_b>(col_left->getData(), const_b, vec_res, scale_a, scale_b);
                    else
                        OpImpl::template vectorConstant<dec_a, dec_b>(col_left->getData(), const_b, vec_res, scale_a, scale_b);
                }
                else
                    return nullptr;
            }
            else
            {
                using OpImpl = BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>;

                /// non-vector result
                if (col_left_const && col_right_const)
                {
                    auto res = OpImpl::constantConstant(col_left_const->template getValue<T0>(), col_right_const->template getValue<T1>());
                    return ResultDataType().createColumnConst(col_left_const->size(), toField(res));
                }

                col_res = ColVecResult::create();
                auto & vec_res = col_res->getData();
                vec_res.resize(col_left_raw->size());

                if (col_left && col_right)
                {
                    OpImpl::vectorVector(col_left->getData().data(), col_right->getData().data(), vec_res.data(), vec_res.size());
                }
                else if (col_left_const && col_right)
                {
                    OpImpl::constantVector(col_left_const->template getValue<T0>(), col_right->getData().data(), vec_res.data(), vec_res.size());
                }
                else if (col_left && col_right_const)
                {
                    OpImpl::vectorConstant(col_left->getData().data(), col_right_const->template getValue<T1>(), vec_res.data(), vec_res.size());
                }
                else
                    return nullptr;
            }

            return col_res;
        }
        return nullptr;
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
        const auto * left_generic = left_argument.type.get();
        const auto * right_generic = right_argument.type.get();
        ColumnPtr res;
        bool valid = castBothTypes(left_generic, right_generic, [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType> || std::is_same_v<DataTypeFixedString, RightDataType>)
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

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, ValuePlaceholders values) const override
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
                    auto * lval = nativeCast(b, types[0], values[0](), type);
                    auto * rval = nativeCast(b, types[1], values[1](), type);
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


template <template <typename, typename> class Op, typename Name, bool valid_on_default_arguments = true>
class FunctionBinaryArithmeticWithConstants : public FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments>
{
public:
    using Base = FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments>;
    using Monotonicity = typename Base::Monotonicity;

    static FunctionPtr create(
        const ColumnWithTypeAndName & left_,
        const ColumnWithTypeAndName & right_,
        const DataTypePtr & return_type_,
        const Context & context)
    {
        return std::make_shared<FunctionBinaryArithmeticWithConstants>(left_, right_, return_type_, context);
    }

    FunctionBinaryArithmeticWithConstants(
        const ColumnWithTypeAndName & left_,
        const ColumnWithTypeAndName & right_,
        const DataTypePtr & return_type_,
        const Context & context_)
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
        std::string_view name_ = Name::name;
        return (name_ == "minus" || name_ == "plus" || name_ == "divide" || name_ == "intDiv");
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field & left_point, const Field & right_point) const override
    {
        // For simplicity, we treat null values as monotonicity breakers.
        if (left_point.isNull() || right_point.isNull())
            return {false, true, false};

        // For simplicity, we treat every single value interval as positive monotonic.
        if (applyVisitor(FieldVisitorAccurateEquals(), left_point, right_point))
            return {true, true, false};

        std::string_view name_ = Name::name;
        if (name_ == "minus" || name_ == "plus")
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
                if (name_ == "plus")
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
        if (name_ == "divide" || name_ == "intDiv")
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


template <template <typename, typename> class Op, typename Name, bool valid_on_default_arguments = true>
class BinaryArithmeticOverloadResolver : public IFunctionOverloadResolverImpl
{
public:
    static constexpr auto name = Name::name;
    static FunctionOverloadResolverImplPtr create(const Context & context)
    {
        return std::make_unique<BinaryArithmeticOverloadResolver>(context);
    }

    explicit BinaryArithmeticOverloadResolver(const Context & context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return false; }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        /// More efficient specialization for two numeric arguments.
        if (arguments.size() == 2
            && ((arguments[0].column && isColumnConst(*arguments[0].column))
                || (arguments[1].column && isColumnConst(*arguments[1].column))))
        {
            return std::make_unique<DefaultFunction>(
                FunctionBinaryArithmeticWithConstants<Op, Name, valid_on_default_arguments>::create(
                    arguments[0], arguments[1], return_type, context),
                ext::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
        }

        return std::make_unique<DefaultFunction>(
            FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments>::create(context),
            ext::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
            return_type);
    }

    DataTypePtr getReturnType(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size()) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        return FunctionBinaryArithmetic<Op, Name, valid_on_default_arguments>::getReturnTypeImplStatic(arguments, context);
    }

private:
    const Context & context;
};

}
