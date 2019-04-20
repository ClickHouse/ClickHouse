#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/Native.h>
#include <DataTypes/NumberTraits.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnAggregateFunction.h>
#include "IFunction.h"
#include "FunctionHelpers.h"
#include "intDiv.h"
#include "castTypeToEither.h"
#include "FunctionFactory.h"
#include <Common/typeid_cast.h>
#include <Common/Arena.h>
#include <Common/config.h>

#if USE_EMBEDDED_COMPILER
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <llvm/IR/IRBuilder.h> // Y_IGNORE
#pragma GCC diagnostic pop
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
    extern const int ILLEGAL_DIVISION;
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

    static void NO_INLINE vector_vector(const PaddedPODArray<A> & a, const PaddedPODArray<B> & b, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::template apply<ResultType>(a[i], b[i]);
    }

    static void NO_INLINE vector_constant(const PaddedPODArray<A> & a, B b, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::template apply<ResultType>(a[i], b);
    }

    static void NO_INLINE constant_vector(A a, const PaddedPODArray<B> & b, PaddedPODArray<ResultType> & c)
    {
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::template apply<ResultType>(a, b[i]);
    }

    static ResultType constant_constant(A a, B b)
    {
        return Op::template apply<ResultType>(a, b);
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


template <typename T> struct NativeType { using Type = T; };
template <> struct NativeType<Decimal32> { using Type = Int32; };
template <> struct NativeType<Decimal64> { using Type = Int64; };
template <> struct NativeType<Decimal128> { using Type = Int128; };

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

    static void vector_vector(const ArrayA & a, const ArrayB & b, ArrayC & c, ResultType scale_a, ResultType scale_b, bool check_overflow)
    {
        if (check_overflow)
            vector_vector(a, b, c, scale_a, scale_b);
        else
            SelfNoOverflow::vector_vector(a, b, c, scale_a, scale_b);
    }

    static void vector_constant(const ArrayA & a, B b, ArrayC & c, ResultType scale_a, ResultType scale_b, bool check_overflow)
    {
        if (check_overflow)
            vector_constant(a, b, c, scale_a, scale_b);
        else
            SelfNoOverflow::vector_constant(a, b, c, scale_a, scale_b);
    }

    static void constant_vector(A a, const ArrayB & b, ArrayC & c, ResultType scale_a, ResultType scale_b, bool check_overflow)
    {
        if (check_overflow)
            constant_vector(a, b, c, scale_a, scale_b);
        else
            SelfNoOverflow::constant_vector(a, b, c, scale_a, scale_b);
    }

    static ResultType constant_constant(A a, B b, ResultType scale_a, ResultType scale_b, bool check_overflow)
    {
        if (check_overflow)
            return constant_constant(a, b, scale_a, scale_b);
        else
            return SelfNoOverflow::constant_constant(a, b, scale_a, scale_b);
    }

    static void NO_INLINE vector_vector(const ArrayA & a, const ArrayB & b, ArrayC & c,
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

    static void NO_INLINE vector_constant(const ArrayA & a, B b, ArrayC & c,
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

    static void NO_INLINE constant_vector(A a, const ArrayB & b, ArrayC & c,
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

    static ResultType constant_constant(A a, B b, ResultType scale_a [[maybe_unused]], ResultType scale_b [[maybe_unused]])
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
template <> constexpr bool IsIntegral<DataTypeUInt8> = true;
template <> constexpr bool IsIntegral<DataTypeUInt16> = true;
template <> constexpr bool IsIntegral<DataTypeUInt32> = true;
template <> constexpr bool IsIntegral<DataTypeUInt64> = true;
template <> constexpr bool IsIntegral<DataTypeInt8> = true;
template <> constexpr bool IsIntegral<DataTypeInt16> = true;
template <> constexpr bool IsIntegral<DataTypeInt32> = true;
template <> constexpr bool IsIntegral<DataTypeInt64> = true;

template <typename DataType> constexpr bool IsFloatingPoint = false;
template <> constexpr bool IsFloatingPoint<DataTypeFloat32> = true;
template <> constexpr bool IsFloatingPoint<DataTypeFloat64> = true;

template <typename DataType> constexpr bool IsDateOrDateTime = false;
template <> constexpr bool IsDateOrDateTime<DataTypeDate> = true;
template <> constexpr bool IsDateOrDateTime<DataTypeDateTime> = true;

template <typename T0, typename T1> constexpr bool UseLeftDecimal = false;
template <> constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal32>> = true;
template <> constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal64>> = true;
template <> constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal64>, DataTypeDecimal<Decimal32>> = true;

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
        /// Date % Int32 -> int32
        Case<std::is_same_v<Op, ModuloImpl<T0, T1>>, Switch<
            Case<IsDateOrDateTime<LeftDataType> && IsIntegral<RightDataType>, RightDataType>,
            Case<IsDateOrDateTime<LeftDataType> && IsFloatingPoint<RightDataType>, DataTypeInt32>>>>;
};


template <template <typename, typename> class Op, typename Name, bool CanBeExecutedOnDefaultArguments = true>
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
            DataTypeDecimal<Decimal128>
        >(type, std::forward<F>(f));
    }

    template <typename F>
    static bool castBothTypes(const IDataType * left, const IDataType * right, F && f)
    {
        return castType(left, [&](const auto & left_) { return castType(right, [&](const auto & right_) { return f(left_, right_); }); });
    }

    FunctionBuilderPtr getFunctionForIntervalArithmetic(const DataTypePtr & type0, const DataTypePtr & type1) const
    {
        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        /// We construct another function (example: addMonths) and call it.

        bool function_is_plus = std::is_same_v<Op<UInt8, UInt8>, PlusImpl<UInt8, UInt8>>;
        bool function_is_minus = std::is_same_v<Op<UInt8, UInt8>, MinusImpl<UInt8, UInt8>>;

        if (!function_is_plus && !function_is_minus)
            return {};

        int interval_arg = 1;
        const DataTypeInterval * interval_data_type = checkAndGetDataType<DataTypeInterval>(type1.get());
        if (!interval_data_type)
        {
            interval_arg = 0;
            interval_data_type = checkAndGetDataType<DataTypeInterval>(type0.get());
        }
        if (!interval_data_type)
            return {};

        if (interval_arg == 0 && function_is_minus)
            throw Exception("Wrong order of arguments for function " + getName() + ": argument of type Interval cannot be first.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypeDate * date_data_type = checkAndGetDataType<DataTypeDate>(interval_arg == 0 ? type1.get() : type0.get());
        const DataTypeDateTime * date_time_data_type = nullptr;
        if (!date_data_type)
        {
            date_time_data_type = checkAndGetDataType<DataTypeDateTime>(interval_arg == 0 ? type1.get() : type0.get());
            if (!date_time_data_type)
                throw Exception("Wrong argument types for function " + getName() + ": if one argument is Interval, then another must be Date or DateTime.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        std::stringstream function_name;
        function_name << (function_is_plus ? "add" : "subtract") << interval_data_type->kindToString() << 's';

        return FunctionFactory::instance().get(function_name.str(), context);
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

        if (!block.getByPosition(new_arguments[1]).column->isColumnConst())
            throw Exception{"Illegal column " + block.getByPosition(new_arguments[1]).column->getName()
                + " of argument of aggregation state multiply. Should be integer constant", ErrorCodes::ILLEGAL_COLUMN};

        const IColumn & agg_state_column = *block.getByPosition(new_arguments[0]).column;
        bool agg_state_is_const = agg_state_column.isColumnConst();
        const ColumnAggregateFunction & column = typeid_cast<const ColumnAggregateFunction &>(
            agg_state_is_const ? static_cast<const ColumnConst &>(agg_state_column).getDataColumn() : agg_state_column);

        AggregateFunctionPtr function = column.getAggregateFunction();

        auto arena = std::make_shared<Arena>();

        size_t size = agg_state_is_const ? 1 : input_rows_count;

        auto column_to = ColumnAggregateFunction::create(function, Arenas(1, arena));
        column_to->reserve(size);

        auto column_from = ColumnAggregateFunction::create(function, Arenas(1, arena));
        column_from->reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            column_to->insertDefault();
            column_from->insertFrom(column.getData()[i]);
        }

        auto & vec_to = column_to->getData();
        auto & vec_from = column_from->getData();

        UInt64 m = typeid_cast<const ColumnConst *>(block.getByPosition(new_arguments[1]).column.get())->getValue<UInt64>();

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

        bool lhs_is_const = lhs_column.isColumnConst();
        bool rhs_is_const = rhs_column.isColumnConst();

        const ColumnAggregateFunction & lhs = typeid_cast<const ColumnAggregateFunction &>(
            lhs_is_const ? static_cast<const ColumnConst &>(lhs_column).getDataColumn() : lhs_column);
        const ColumnAggregateFunction & rhs = typeid_cast<const ColumnAggregateFunction &>(
            rhs_is_const ? static_cast<const ColumnConst &>(rhs_column).getDataColumn() : rhs_column);

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
        size_t result, size_t input_rows_count, const FunctionBuilderPtr & function_builder) const
    {
        ColumnNumbers new_arguments = arguments;

        /// Interval argument must be second.
        if (WhichDataType(block.getByPosition(arguments[0]).type).isInterval())
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
            if (WhichDataType(new_arguments[0].type).isInterval())
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
                else
                    type_res = std::make_shared<ResultDataType>();
                return true;
            }
            return false;
        });
        if (!valid)
            throw Exception("Illegal types " + arguments[0]->getName() + " and " + arguments[1]->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return type_res;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
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

        auto * left_generic = block.getByPosition(arguments[0]).type.get();
        auto * right_generic = block.getByPosition(arguments[1]).type.get();
        bool valid = castBothTypes(left_generic, right_generic, [&](const auto & left, const auto & right)
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

                            auto res = OpImpl::constant_constant(col_left->template getValue<T0>(), col_right->template getValue<T1>(),
                                                                    scale_a, scale_b, check_decimal_overflow);
                            block.getByPosition(result).column =
                                ResultDataType(type.getPrecision(), type.getScale()).createColumnConst(
                                    col_left->size(), toField(res, type.getScale()));

                        }
                        else
                        {
                            auto res = OpImpl::constant_constant(col_left->template getValue<T0>(), col_right->template getValue<T1>());
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

                            OpImpl::constant_vector(col_left_const->template getValue<T0>(), col_right->getData(), vec_res,
                                                    scale_a, scale_b, check_decimal_overflow);
                        }
                        else
                            OpImpl::constant_vector(col_left_const->template getValue<T0>(), col_right->getData(), vec_res);
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
                            OpImpl::vector_vector(col_left->getData(), col_right->getData(), vec_res, scale_a, scale_b,
                                                  check_decimal_overflow);
                        }
                        else if (auto col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw))
                        {
                            OpImpl::vector_constant(col_left->getData(), col_right_const->template getValue<T1>(), vec_res,
                                                    scale_a, scale_b, check_decimal_overflow);
                        }
                        else
                            return false;
                    }
                    else
                    {
                        if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                            OpImpl::vector_vector(col_left->getData(), col_right->getData(), vec_res);
                        else if (auto col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_raw))
                            OpImpl::vector_constant(col_left->getData(), col_right_const->template getValue<T1>(), vec_res);
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
        });
        if (!valid)
            throw Exception(getName() + "'s arguments do not match the expected data types", ErrorCodes::LOGICAL_ERROR);
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & arguments) const override
    {
        return castBothTypes(arguments[0].get(), arguments[1].get(), [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;
            using OpSpec = Op<typename LeftDataType::FieldType, typename RightDataType::FieldType>;
            return !std::is_same_v<ResultDataType, InvalidType> && !IsDataTypeDecimal<ResultDataType> && OpSpec::compilable;
        });
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, ValuePlaceholders values) const override
    {
        llvm::Value * result = nullptr;
        castBothTypes(types[0].get(), types[1].get(), [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
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
            return false;
        });
        return result;
    }
#endif

    bool canBeExecutedOnDefaultArguments() const override { return CanBeExecutedOnDefaultArguments; }
};

}
