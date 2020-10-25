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
#include "DivisionUtils.h"
#include "castTypeToEither.h"
#include "FunctionFactory.h"
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

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
}


/** Arithmetic operations: +, -, *, /, %,
  * intDiv (integer division)
  * Bitwise operations: |, &, ^, ~.
  * Etc.
  */

template <typename A, typename B, typename Op, typename ResultType_ = typename Op::ResultType>
struct BinaryOperationImplBase
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
struct BinaryOperationImpl : BinaryOperationImplBase<A, B, Op, ResultType>
{
};


template <typename, typename> struct PlusImpl;
template <typename, typename> struct MinusImpl;
template <typename, typename> struct MultiplyImpl;
template <typename, typename> struct DivideFloatingImpl;
template <typename, typename> struct DivideIntegralImpl;
template <typename, typename> struct DivideIntegralOrZeroImpl;
template <typename, typename> struct LeastBaseImpl;
template <typename, typename> struct GreatestBaseImpl;
template <typename, typename> struct ModuloImpl;


/// Binary operations for Decimals need scale args
/// +|- scale one of args (which scale factor is not 1). ScaleR = oneof(Scale1, Scale2);
/// *   no agrs scale. ScaleR = Scale1 + Scale2;
/// /   first arg scale. ScaleR = Scale1 (scale_a = DecimalType<B>::getScale()).
template <typename A, typename B, template <typename, typename> typename Operation, typename ResultType_, bool _check_overflow = true>
struct DecimalBinaryOperation
{
    static constexpr bool is_plus_minus =   std::is_same_v<Operation<Int32, Int32>, PlusImpl<Int32, Int32>> ||
                                            std::is_same_v<Operation<Int32, Int32>, MinusImpl<Int32, Int32>>;
    static constexpr bool is_multiply =     std::is_same_v<Operation<Int32, Int32>, MultiplyImpl<Int32, Int32>>;
    static constexpr bool is_float_division = std::is_same_v<Operation<Int32, Int32>, DivideFloatingImpl<Int32, Int32>>;
    static constexpr bool is_int_division = std::is_same_v<Operation<Int32, Int32>, DivideIntegralImpl<Int32, Int32>> ||
                                            std::is_same_v<Operation<Int32, Int32>, DivideIntegralOrZeroImpl<Int32, Int32>>;
    static constexpr bool is_division = is_float_division || is_int_division;
    static constexpr bool is_compare =      std::is_same_v<Operation<Int32, Int32>, LeastBaseImpl<Int32, Int32>> ||
                                            std::is_same_v<Operation<Int32, Int32>, GreatestBaseImpl<Int32, Int32>>;
    static constexpr bool is_plus_minus_compare = is_plus_minus || is_compare;
    static constexpr bool can_overflow = is_plus_minus || is_multiply;

    using ResultType = ResultType_;
    using NativeResultType = typename NativeType<ResultType>::Type;
    using Op = std::conditional_t<is_float_division,
        DivideIntegralImpl<NativeResultType, NativeResultType>, /// substitute divide by intDiv (throw on division by zero)
        Operation<NativeResultType, NativeResultType>>;
    using ColVecA = std::conditional_t<IsDecimalNumber<A>, ColumnDecimal<A>, ColumnVector<A>>;
    using ColVecB = std::conditional_t<IsDecimalNumber<B>, ColumnDecimal<B>, ColumnVector<B>>;
    using ArrayA = typename ColVecA::Container;
    using ArrayB = typename ColVecB::Container;
    using ArrayC = typename ColumnDecimal<ResultType>::Container;
    using SelfNoOverflow = DecimalBinaryOperation<A, B, Operation, ResultType_, false>;

    static void vectorVector(const ArrayA & a, const ArrayB & b, ArrayC & c, ResultType scale_a, ResultType scale_b, bool check_overflow)
    {
        if (check_overflow)
            vectorVector(a, b, c, scale_a, scale_b);
        else
            SelfNoOverflow::vectorVector(a, b, c, scale_a, scale_b);
    }

    static void vectorConstant(const ArrayA & a, B b, ArrayC & c, ResultType scale_a, ResultType scale_b, bool check_overflow)
    {
        if (check_overflow)
            vectorConstant(a, b, c, scale_a, scale_b);
        else
            SelfNoOverflow::vectorConstant(a, b, c, scale_a, scale_b);
    }

    static void constantVector(A a, const ArrayB & b, ArrayC & c, ResultType scale_a, ResultType scale_b, bool check_overflow)
    {
        if (check_overflow)
            constantVector(a, b, c, scale_a, scale_b);
        else
            SelfNoOverflow::constantVector(a, b, c, scale_a, scale_b);
    }

    static ResultType constantConstant(A a, B b, ResultType scale_a, ResultType scale_b, bool check_overflow)
    {
        if (check_overflow)
            return constantConstant(a, b, scale_a, scale_b);
        else
            return SelfNoOverflow::constantConstant(a, b, scale_a, scale_b);
    }

    static void NO_INLINE vectorVector(const ArrayA & a, const ArrayB & b, ArrayC & c,
                                        ResultType scale_a [[maybe_unused]], ResultType scale_b [[maybe_unused]])
    {
        size_t size = a.size();
        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a[i], b[i], scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a[i], b[i], scale_b);
                return;
            }
        }
        else if constexpr (is_division && IsDecimalNumber<B>)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledDiv(a[i], b[i], scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a[i], b[i]);
    }

    static void NO_INLINE vectorConstant(const ArrayA & a, B b, ArrayC & c,
                                        ResultType scale_a [[maybe_unused]], ResultType scale_b [[maybe_unused]])
    {
        size_t size = a.size();
        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a[i], b, scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a[i], b, scale_b);
                return;
            }
        }
        else if constexpr (is_division && IsDecimalNumber<B>)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledDiv(a[i], b, scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a[i], b);
    }

    static void NO_INLINE constantVector(A a, const ArrayB & b, ArrayC & c,
                                        ResultType scale_a [[maybe_unused]], ResultType scale_b [[maybe_unused]])
    {
        size_t size = b.size();
        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<true>(a, b[i], scale_a);
                return;
            }
            else if (scale_b != 1)
            {
                for (size_t i = 0; i < size; ++i)
                    c[i] = applyScaled<false>(a, b[i], scale_b);
                return;
            }
        }
        else if constexpr (is_division && IsDecimalNumber<B>)
        {
            for (size_t i = 0; i < size; ++i)
                c[i] = applyScaledDiv(a, b[i], scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i)
            c[i] = apply(a, b[i]);
    }

    static ResultType constantConstant(A a, B b, ResultType scale_a [[maybe_unused]], ResultType scale_b [[maybe_unused]])
    {
        if constexpr (is_plus_minus_compare)
        {
            if (scale_a != 1)
                return applyScaled<true>(a, b, scale_a);
            else if (scale_b != 1)
                return applyScaled<false>(a, b, scale_b);
        }
        else if constexpr (is_division && IsDecimalNumber<B>)
            return applyScaledDiv(a, b, scale_a);
        return apply(a, b);
    }

private:
    /// there's implicit type convertion here
    static NativeResultType apply(NativeResultType a, NativeResultType b)
    {
        if constexpr (can_overflow && _check_overflow)
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

            if constexpr (_check_overflow)
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

    static NO_SANITIZE_UNDEFINED NativeResultType applyScaledDiv(NativeResultType a, NativeResultType b, NativeResultType scale)
    {
        if constexpr (is_division)
        {
            if constexpr (_check_overflow)
            {
                bool overflow = false;
                if constexpr (!IsDecimalNumber<A>)
                    overflow |= common::mulOverflow(scale, scale, scale);
                overflow |= common::mulOverflow(a, scale, a);
                if (overflow)
                    throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
            }
            else
            {
                if constexpr (!IsDecimalNumber<A>)
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

template <typename DataType> constexpr bool IsFloatingPoint = false;
template <> inline constexpr bool IsFloatingPoint<DataTypeFloat32> = true;
template <> inline constexpr bool IsFloatingPoint<DataTypeFloat64> = true;

template <typename DataType> constexpr bool IsDateOrDateTime = false;
template <> inline constexpr bool IsDateOrDateTime<DataTypeDate> = true;
template <> inline constexpr bool IsDateOrDateTime<DataTypeDateTime> = true;

template <typename T0, typename T1> constexpr bool UseLeftDecimal = false;
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

    static constexpr bool allow_decimal =
        std::is_same_v<Operation<T0, T0>, PlusImpl<T0, T0>> ||
        std::is_same_v<Operation<T0, T0>, MinusImpl<T0, T0>> ||
        std::is_same_v<Operation<T0, T0>, MultiplyImpl<T0, T0>> ||
        std::is_same_v<Operation<T0, T0>, DivideFloatingImpl<T0, T0>> ||
        std::is_same_v<Operation<T0, T0>, DivideIntegralImpl<T0, T0>> ||
        std::is_same_v<Operation<T0, T0>, DivideIntegralOrZeroImpl<T0, T0>> ||
        std::is_same_v<Operation<T0, T0>, LeastBaseImpl<T0, T0>> ||
        std::is_same_v<Operation<T0, T0>, GreatestBaseImpl<T0, T0>>;

    /// Appropriate result type for binary operator on numeric types. "Date" can also mean
    /// DateTime, but if both operands are Dates, their type must be the same (e.g. Date - DateTime is invalid).
    using ResultDataType = Switch<
        /// Decimal cases
        Case<!allow_decimal && (IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>), InvalidType>,
        Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> && UseLeftDecimal<LeftDataType, RightDataType>, LeftDataType>,
        Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType>, RightDataType>,
        Case<IsDataTypeDecimal<LeftDataType> && !IsDataTypeDecimal<RightDataType> && IsIntegral<RightDataType>, LeftDataType>,
        Case<!IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> && IsIntegral<LeftDataType>, RightDataType>,
        /// Decimal <op> Real is not supported (traditional DBs convert Decimal <op> Real to Real)
        Case<IsDataTypeDecimal<LeftDataType> && !IsDataTypeDecimal<RightDataType> && !IsIntegral<RightDataType>, InvalidType>,
        Case<!IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> && !IsIntegral<LeftDataType>, InvalidType>,
        /// number <op> number -> see corresponding impl
        Case<!IsDateOrDateTime<LeftDataType> && !IsDateOrDateTime<RightDataType>,
            DataTypeFromFieldType<typename Op::ResultType>>,
        /// Date + Integral -> Date
        /// Integral + Date -> Date
        Case<std::is_same_v<Op, PlusImpl<T0, T1>>, Switch<
            Case<IsIntegral<RightDataType>, LeftDataType>,
            Case<IsIntegral<LeftDataType>, RightDataType>>>,
        /// Date - Date     -> Int32
        /// Date - Integral -> Date
        Case<std::is_same_v<Op, MinusImpl<T0, T1>>, Switch<
            Case<std::is_same_v<LeftDataType, RightDataType>, DataTypeInt32>,
            Case<IsDateOrDateTime<LeftDataType> && IsIntegral<RightDataType>, LeftDataType>>>,
        /// least(Date, Date) -> Date
        /// greatest(Date, Date) -> Date
        Case<std::is_same_v<LeftDataType, RightDataType> && (std::is_same_v<Op, LeastBaseImpl<T0, T1>> || std::is_same_v<Op, GreatestBaseImpl<T0, T1>>),
            LeftDataType>,
        /// Date % Int32 -> Int32
        /// Date % Float -> Float64
        Case<std::is_same_v<Op, ModuloImpl<T0, T1>>, Switch<
            Case<IsDateOrDateTime<LeftDataType> && IsIntegral<RightDataType>, RightDataType>,
            Case<IsDateOrDateTime<LeftDataType> && IsFloatingPoint<RightDataType>, DataTypeFloat64>>>>;
};


template <template <typename, typename> class Op, typename Name, bool valid_on_default_arguments = true>
class FunctionBinaryArithmetic : public IFunction
{
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
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeFloat32,
            DataTypeFloat64,
            DataTypeDate,
            DataTypeDateTime,
            DataTypeDecimal<Decimal32>,
            DataTypeDecimal<Decimal64>,
            DataTypeDecimal<Decimal128>,
            DataTypeFixedString
        >(type, std::forward<F>(f));
    }

    template <typename F>
    static bool castBothTypes(const IDataType * left, const IDataType * right, F && f)
    {
        return castType(left, [&](const auto & left_) { return castType(right, [&](const auto & right_) { return f(left_, right_); }); });
    }

    FunctionOverloadResolverPtr getFunctionForIntervalArithmetic(const DataTypePtr & type0, const DataTypePtr & type1) const
    {
        bool first_is_date_or_datetime = isDateOrDateTime(type0);
        bool second_is_date_or_datetime = isDateOrDateTime(type1);

        /// Exactly one argument must be Date or DateTime
        if (first_is_date_or_datetime == second_is_date_or_datetime)
            return {};

        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        /// We construct another function (example: addMonths) and call it.

        static constexpr bool function_is_plus = std::is_same_v<Op<UInt8, UInt8>, PlusImpl<UInt8, UInt8>>;
        static constexpr bool function_is_minus = std::is_same_v<Op<UInt8, UInt8>, MinusImpl<UInt8, UInt8>>;

        if (!function_is_plus && !function_is_minus)
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

        if (second_is_date_or_datetime && function_is_minus)
            throw Exception("Wrong order of arguments for function " + getName() + ": argument of type Interval cannot be first.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        std::string function_name;
        if (interval_data_type)
        {
            function_name = String(function_is_plus ? "add" : "subtract") + interval_data_type->getKind().toString() + 's';
        }
        else
        {
            if (isDate(type_time))
                function_name = function_is_plus ? "addDays" : "subtractDays";
            else
                function_name = function_is_plus ? "addSeconds" : "subtractSeconds";
        }

        return FunctionFactory::instance().get(function_name, context);
    }

    bool isAggregateMultiply(const DataTypePtr & type0, const DataTypePtr & type1) const
    {
        if constexpr (!std::is_same_v<Op<UInt8, UInt8>, MultiplyImpl<UInt8, UInt8>>)
            return false;

        WhichDataType which0(type0);
        WhichDataType which1(type1);

        return (which0.isAggregateFunction() && which1.isNativeUInt())
            || (which0.isNativeUInt() && which1.isAggregateFunction());
    }

    bool isAggregateAddition(const DataTypePtr & type0, const DataTypePtr & type1) const
    {
        if constexpr (!std::is_same_v<Op<UInt8, UInt8>, PlusImpl<UInt8, UInt8>>)
            return false;

        WhichDataType which0(type0);
        WhichDataType which1(type1);

        return which0.isAggregateFunction() && which1.isAggregateFunction();
    }

    /// Multiply aggregation state by integer constant: by merging it with itself specified number of times.
    void executeAggregateMultiply(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const
    {
        ColumnNumbers new_arguments = arguments;
        if (WhichDataType(block.getByPosition(new_arguments[1]).type).isAggregateFunction())
            std::swap(new_arguments[0], new_arguments[1]);

        if (!isColumnConst(*block.getByPosition(new_arguments[1]).column))
            throw Exception{"Illegal column " + block.getByPosition(new_arguments[1]).column->getName()
                + " of argument of aggregation state multiply. Should be integer constant", ErrorCodes::ILLEGAL_COLUMN};

        const IColumn & agg_state_column = *block.getByPosition(new_arguments[0]).column;
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

        UInt64 m = typeid_cast<const ColumnConst *>(block.getByPosition(new_arguments[1]).column.get())->getValue<UInt64>();

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
            block.getByPosition(result).column = ColumnConst::create(std::move(column_to), input_rows_count);
        else
            block.getByPosition(result).column = std::move(column_to);
    }

    /// Merge two aggregation states together.
    void executeAggregateAddition(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const
    {
        const IColumn & lhs_column = *block.getByPosition(arguments[0]).column;
        const IColumn & rhs_column = *block.getByPosition(arguments[1]).column;

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
            block.getByPosition(result).column = ColumnConst::create(std::move(column_to), input_rows_count);
        else
            block.getByPosition(result).column = std::move(column_to);
    }

    void executeDateTimeIntervalPlusMinus(Block & block, const ColumnNumbers & arguments,
        size_t result, size_t input_rows_count, const FunctionOverloadResolverPtr & function_builder) const
    {
        ColumnNumbers new_arguments = arguments;

        /// Interval argument must be second.
        if (WhichDataType(block.getByPosition(arguments[1]).type).isDateOrDateTime())
            std::swap(new_arguments[0], new_arguments[1]);

        /// Change interval argument type to its representation
        Block new_block = block;
        new_block.getByPosition(new_arguments[1]).type = std::make_shared<DataTypeNumber<DataTypeInterval::FieldType>>();

        ColumnsWithTypeAndName new_arguments_with_type_and_name =
                {new_block.getByPosition(new_arguments[0]), new_block.getByPosition(new_arguments[1])};
        auto function = function_builder->build(new_arguments_with_type_and_name);

        function->execute(new_block, new_arguments, result, input_rows_count);
        block.getByPosition(result).column = new_block.getByPosition(result).column;
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBinaryArithmetic>(context); }

    FunctionBinaryArithmetic(const Context & context_)
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
        if (auto function_builder = getFunctionForIntervalArithmetic(arguments[0], arguments[1]))
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
            return function->getReturnType();
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
                        constexpr bool is_multiply = std::is_same_v<Op<UInt8, UInt8>, MultiplyImpl<UInt8, UInt8>>;
                        constexpr bool is_division = std::is_same_v<Op<UInt8, UInt8>, DivideFloatingImpl<UInt8, UInt8>> ||
                                                   std::is_same_v<Op<UInt8, UInt8>, DivideIntegralImpl<UInt8, UInt8>> ||
                                                   std::is_same_v<Op<UInt8, UInt8>, DivideIntegralOrZeroImpl<UInt8, UInt8>>;

                        ResultDataType result_type = decimalResultType(left, right, is_multiply, is_division);
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
            throw Exception("Illegal types " + arguments[0]->getName() + " and " + arguments[1]->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return type_res;
    }

    bool executeFixedString(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        using OpImpl = FixedStringOperationImpl<Op<UInt8, UInt8>>;

        auto col_left_raw = block.getByPosition(arguments[0]).column.get();
        auto col_right_raw = block.getByPosition(arguments[1]).column.get();
        if (auto col_left_const = checkAndGetColumnConst<ColumnFixedString>(col_left_raw))
        {
            if (auto col_right_const = checkAndGetColumnConst<ColumnFixedString>(col_right_raw))
            {
                auto col_left = checkAndGetColumn<ColumnFixedString>(col_left_const->getDataColumn());
                auto col_right = checkAndGetColumn<ColumnFixedString>(col_right_const->getDataColumn());
                if (col_left->getN() != col_right->getN())
                    return false;
                auto col_res = ColumnFixedString::create(col_left->getN());
                auto & out_chars = col_res->getChars();
                out_chars.resize(col_left->getN());
                OpImpl::vectorVector(col_left->getChars().data(),
                                      col_right->getChars().data(),
                                      out_chars.data(),
                                      out_chars.size());
                block.getByPosition(result).column = ColumnConst::create(std::move(col_res), block.rows());
                return true;
            }
        }

        bool is_left_column_const = checkAndGetColumnConst<ColumnFixedString>(col_left_raw) != nullptr;
        bool is_right_column_const = checkAndGetColumnConst<ColumnFixedString>(col_right_raw) != nullptr;

        auto col_left = is_left_column_const
                        ? checkAndGetColumn<ColumnFixedString>(checkAndGetColumnConst<ColumnFixedString>(col_left_raw)->getDataColumn())
                        : checkAndGetColumn<ColumnFixedString>(col_left_raw);
        auto col_right = is_right_column_const
                        ? checkAndGetColumn<ColumnFixedString>(checkAndGetColumnConst<ColumnFixedString>(col_right_raw)->getDataColumn())
                        : checkAndGetColumn<ColumnFixedString>(col_right_raw);

        if (col_left && col_right)
        {
            if (col_left->getN() != col_right->getN())
                return false;

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
            block.getByPosition(result).column = std::move(col_res);
            return true;
        }
        return false;
    }

    template <typename A, typename B>
    bool executeNumeric(Block & block, const ColumnNumbers & arguments, size_t result [[maybe_unused]], const A & left, const B & right) const
    {
        using LeftDataType = std::decay_t<decltype(left)>;
        using RightDataType = std::decay_t<decltype(right)>;
        using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

        if constexpr (!std::is_same_v<ResultDataType, InvalidType>)
        {
            constexpr bool result_is_decimal = IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>;
            constexpr bool is_multiply = std::is_same_v<Op<UInt8, UInt8>, MultiplyImpl<UInt8, UInt8>>;
            constexpr bool is_division = std::is_same_v<Op<UInt8, UInt8>, DivideFloatingImpl<UInt8, UInt8>> ||
                                            std::is_same_v<Op<UInt8, UInt8>, DivideIntegralImpl<UInt8, UInt8>> ||
                                            std::is_same_v<Op<UInt8, UInt8>, DivideIntegralOrZeroImpl<UInt8, UInt8>>;

            using T0 = typename LeftDataType::FieldType;
            using T1 = typename RightDataType::FieldType;
            using ResultType = typename ResultDataType::FieldType;
            using ColVecT0 = std::conditional_t<IsDecimalNumber<T0>, ColumnDecimal<T0>, ColumnVector<T0>>;
            using ColVecT1 = std::conditional_t<IsDecimalNumber<T1>, ColumnDecimal<T1>, ColumnVector<T1>>;
            using ColVecResult = std::conditional_t<IsDecimalNumber<ResultType>, ColumnDecimal<ResultType>, ColumnVector<ResultType>>;

            /// Decimal operations need scale. Operations are on result type.
            using OpImpl = std::conditional_t<IsDataTypeDecimal<ResultDataType>,
                DecimalBinaryOperation<T0, T1, Op, ResultType>,
                BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>>;

            auto col_left_raw = block.getByPosition(arguments[0]).column.get();
            auto col_right_raw = block.getByPosition(arguments[1]).column.get();
            if (auto col_left = checkAndGetColumnConst<ColVecT0>(col_left_raw))
            {
                if (auto col_right = checkAndGetColumnConst<ColVecT1>(col_right_raw))
                {
                    /// the only case with a non-vector result
                    if constexpr (result_is_decimal)
                    {
                        ResultDataType type = decimalResultType(left, right, is_multiply, is_division);
                        typename ResultDataType::FieldType scale_a = type.scaleFactorFor(left, is_multiply);
                        typename ResultDataType::FieldType scale_b = type.scaleFactorFor(right, is_multiply || is_division);
                        if constexpr (IsDataTypeDecimal<RightDataType> && is_division)
                            scale_a = right.getScaleMultiplier();

                        auto res = OpImpl::constantConstant(col_left->template getValue<T0>(), col_right->template getValue<T1>(),
                                                                scale_a, scale_b, check_decimal_overflow);
                        block.getByPosition(result).column =
                            ResultDataType(type.getPrecision(), type.getScale()).createColumnConst(
                                col_left->size(), toField(res, type.getScale()));

                    }
                    else
                    {
                        auto res = OpImpl::constantConstant(col_left->template getValue<T0>(), col_right->template getValue<T1>());
                        block.getByPosition(result).column = ResultDataType().createColumnConst(col_left->size(), toField(res));
                    }
                    return true;
                }
            }

            typename ColVecResult::MutablePtr col_res = nullptr;
            if constexpr (result_is_decimal)
            {
                ResultDataType type = decimalResultType(left, right, is_multiply, is_division);
                col_res = ColVecResult::create(0, type.getScale());
            }
            else
                col_res = ColVecResult::create();

            auto & vec_res = col_res->getData();
            vec_res.resize(block.rows());

            if (auto col_left_const = checkAndGetColumnConst<ColVecT0>(col_left_raw))
            {
                if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                {
                    if constexpr (result_is_decimal)
                    {
                        ResultDataType type = decimalResultType(left, right, is_multiply, is_division);

                        typename ResultDataType::FieldType scale_a = type.scaleFactorFor(left, is_multiply);
                        typename ResultDataType::FieldType scale_b = type.scaleFactorFor(right, is_multiply || is_division);
                        if constexpr (IsDataTypeDecimal<RightDataType> && is_division)
                            scale_a = right.getScaleMultiplier();

                        OpImpl::constantVector(col_left_const->template getValue<T0>(), col_right->getData(), vec_res,
                                                scale_a, scale_b, check_decimal_overflow);
                    }
                    else
                        OpImpl::constantVector(col_left_const->template getValue<T0>(), col_right->getData().data(), vec_res.data(), vec_res.size());
                }
                else
                    return false;
            }
            else if (auto col_left = checkAndGetColumn<ColVecT0>(col_left_raw))
            {
                if constexpr (result_is_decimal)
                {
                    ResultDataType type = decimalResultType(left, right, is_multiply, is_division);

                    typename ResultDataType::FieldType scale_a = type.scaleFactorFor(left, is_multiply);
                    typename ResultDataType::FieldType scale_b = type.scaleFactorFor(right, is_multiply || is_division);
                    if constexpr (IsDataTypeDecimal<RightDataType> && is_division)
                        scale_a = right.getScaleMultiplier();
                    if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                    {
                        OpImpl::vectorVector(col_left->getData(), col_right->getData(), vec_res, scale_a, scale_b,
                                              check_decimal_overflow);
                    }
                    else if (auto col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw))
                    {
                        OpImpl::vectorConstant(col_left->getData(), col_right_const->template getValue<T1>(), vec_res,
                                                scale_a, scale_b, check_decimal_overflow);
                    }
                    else
                        return false;
                }
                else
                {
                    if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                        OpImpl::vectorVector(col_left->getData().data(), col_right->getData().data(), vec_res.data(), vec_res.size());
                    else if (auto col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw))
                        OpImpl::vectorConstant(col_left->getData().data(), col_right_const->template getValue<T1>(), vec_res.data(), vec_res.size());
                    else
                        return false;
                }
            }
            else
                return false;

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }
        return false;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        /// Special case when multiply aggregate function state
        if (isAggregateMultiply(block.getByPosition(arguments[0]).type, block.getByPosition(arguments[1]).type))
        {
            executeAggregateMultiply(block, arguments, result, input_rows_count);
            return;
        }

        /// Special case - addition of two aggregate functions states
        if (isAggregateAddition(block.getByPosition(arguments[0]).type, block.getByPosition(arguments[1]).type))
        {
            executeAggregateAddition(block, arguments, result, input_rows_count);
            return;
        }

        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        if (auto function_builder = getFunctionForIntervalArithmetic(block.getByPosition(arguments[0]).type, block.getByPosition(arguments[1]).type))
        {
            executeDateTimeIntervalPlusMinus(block, arguments, result, input_rows_count, function_builder);
            return;
        }

        const auto & left_argument = block.getByPosition(arguments[0]);
        const auto & right_argument = block.getByPosition(arguments[1]);
        auto * left_generic = left_argument.type.get();
        auto * right_generic = right_argument.type.get();
        bool valid = castBothTypes(left_generic, right_generic, [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            if constexpr (std::is_same_v<DataTypeFixedString, LeftDataType> || std::is_same_v<DataTypeFixedString, RightDataType>)
            {
                if constexpr (!Op<DataTypeFixedString, DataTypeFixedString>::allow_fixed_string)
                    return false;
                else
                    return executeFixedString(block, arguments, result);
            }
            else
                return executeNumeric(block, arguments, result, left, right);
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
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & arguments) const override
    {
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

}
