#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeInterval.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/NumberTraits.h>
#include <Core/AccurateComparison.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <ext/range.h>
#include <common/intExp.h>
#include <boost/math/common_factor.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
}


/** Arithmetic operations: +, -, *, /, %,
  * intDiv (integer division), unary minus.
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

    static void constant_constant(A a, B b, ResultType & c)
    {
        c = Op::template apply<ResultType>(a, b);
    }
};

template <typename A, typename B, typename Op, typename ResultType = typename Op::ResultType>
struct BinaryOperationImpl : BinaryOperationImplBase<A, B, Op, ResultType>
{
};


template <typename A, typename Op>
struct UnaryOperationImpl
{
    using ResultType = typename Op::ResultType;

    static void NO_INLINE vector(const PaddedPODArray<A> & a, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::apply(a[i]);
    }

    static void constant(A a, ResultType & c)
    {
        c = Op::apply(a);
    }
};


template <typename A, typename B>
struct PlusImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        /// Next everywhere, static_cast - so that there is no wrong result in expressions of the form Int64 c = UInt32(a) * Int32(-1).
        return static_cast<Result>(a) + b;
    }
};


template <typename A, typename B>
struct MultiplyImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) * b;
    }
};

template <typename A, typename B>
struct MinusImpl
{
    using ResultType = typename NumberTraits::ResultOfSubtraction<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) - b;
    }
};

template <typename A, typename B>
struct DivideFloatingImpl
{
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) / b;
    }
};


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

template <typename A, typename B>
inline void throwIfDivisionLeadsToFPE(A a, B b)
{
    /// Is it better to use siglongjmp instead of checks?

    if (unlikely(b == 0))
        throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

    /// http://avva.livejournal.com/2548306.html
    if (unlikely(std::is_signed_v<A> && std::is_signed_v<B> && a == std::numeric_limits<A>::min() && b == -1))
        throw Exception("Division of minimal signed number by minus one", ErrorCodes::ILLEGAL_DIVISION);
}

template <typename A, typename B>
inline bool divisionLeadsToFPE(A a, B b)
{
    if (unlikely(b == 0))
        return true;

    if (unlikely(std::is_signed_v<A> && std::is_signed_v<B> && a == std::numeric_limits<A>::min() && b == -1))
        return true;

    return false;
}


#pragma GCC diagnostic pop


template <typename A, typename B>
struct DivideIntegralImpl
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(a, b);
        return a / b;
    }
};

template <typename A, typename B>
struct DivideIntegralOrZeroImpl
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return unlikely(divisionLeadsToFPE(a, b)) ? 0 : a / b;
    }
};

template <typename A, typename B>
struct ModuloImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        return typename NumberTraits::ToInteger<A>::Type(a)
            % typename NumberTraits::ToInteger<B>::Type(b);
    }
};

template <typename A, typename B>
struct BitAndImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a)
            & static_cast<Result>(b);
    }
};

template <typename A, typename B>
struct BitOrImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a)
            | static_cast<Result>(b);
    }
};

template <typename A, typename B>
struct BitXorImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a)
            ^ static_cast<Result>(b);
    }
};

template <typename A, typename B>
struct BitShiftLeftImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a)
            << static_cast<Result>(b);
    }
};

template <typename A, typename B>
struct BitShiftRightImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a)
            >> static_cast<Result>(b);
    }
};

template <typename A, typename B>
struct BitRotateLeftImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return (static_cast<Result>(a) << static_cast<Result>(b))
            | (static_cast<Result>(a) >> ((sizeof(Result) * 8) - static_cast<Result>(b)));
    }
};

template <typename A, typename B>
struct BitRotateRightImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return (static_cast<Result>(a) >> static_cast<Result>(b))
            | (static_cast<Result>(a) << ((sizeof(Result) * 8) - static_cast<Result>(b)));
    }
};


template <typename T>
std::enable_if_t<std::is_integral_v<T>, T> toInteger(T x) { return x; }

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, Int64> toInteger(T x) { return Int64(x); }

template <typename A, typename B>
struct BitTestImpl
{
    using ResultType = UInt8;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) { return (toInteger(a) >> toInteger(b)) & 1; };
};


template <typename A, typename B>
struct LeastBaseImpl
{
    using ResultType = NumberTraits::ResultOfLeast<A, B>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        /** gcc 4.9.2 successfully vectorizes a loop from this function. */
        return static_cast<Result>(a) < static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
};

template <typename A, typename B>
struct LeastSpecialImpl
{
    using ResultType = std::make_signed_t<A>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        static_assert(std::is_same_v<Result, ResultType>, "ResultType != Result");
        return accurate::lessOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
};

template <typename A, typename B>
using LeastImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>, LeastBaseImpl<A, B>, LeastSpecialImpl<A, B>>;


template <typename A, typename B>
struct GreatestBaseImpl
{
    using ResultType = NumberTraits::ResultOfGreatest<A, B>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) > static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
};

template <typename A, typename B>
struct GreatestSpecialImpl
{
    using ResultType = std::make_unsigned_t<A>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        static_assert(std::is_same_v<Result, ResultType>, "ResultType != Result");
        return accurate::greaterOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
};

template <typename A, typename B>
using GreatestImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>, GreatestBaseImpl<A, B>, GreatestSpecialImpl<A, B>>;


template <typename A>
struct NegateImpl
{
    using ResultType = typename NumberTraits::ResultOfNegate<A>::Type;

    static inline ResultType apply(A a)
    {
        return -static_cast<ResultType>(a);
    }
};

template <typename A>
struct BitNotImpl
{
    using ResultType = typename NumberTraits::ResultOfBitNot<A>::Type;

    static inline ResultType apply(A a)
    {
        return ~static_cast<ResultType>(a);
    }
};

template <typename A>
struct AbsImpl
{
    using ResultType = typename NumberTraits::ResultOfAbs<A>::Type;

    static inline ResultType apply(A a)
    {
        if constexpr (std::is_integral_v<A> && std::is_signed_v<A>)
            return a < 0 ? static_cast<ResultType>(~a) + 1 : a;
        else if constexpr (std::is_integral_v<A> && std::is_unsigned_v<A>)
            return static_cast<ResultType>(a);
        else if constexpr (std::is_floating_point_v<A>)
            return static_cast<ResultType>(std::abs(a));
    }
};

template <typename A, typename B>
struct GCDImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<B>::Type(b), typename NumberTraits::ToInteger<A>::Type(a));
        return boost::math::gcd(
            typename NumberTraits::ToInteger<Result>::Type(a),
            typename NumberTraits::ToInteger<Result>::Type(b));
    }
};

template <typename A, typename B>
struct LCMImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<B>::Type(b), typename NumberTraits::ToInteger<A>::Type(a));
        return boost::math::lcm(
            typename NumberTraits::ToInteger<Result>::Type(a),
            typename NumberTraits::ToInteger<Result>::Type(b));
    }
};

template <typename A>
struct IntExp2Impl
{
    using ResultType = UInt64;

    static inline ResultType apply(A a)
    {
        return intExp2(a);
    }
};

template <typename A>
struct IntExp10Impl
{
    using ResultType = UInt64;

    static inline ResultType apply(A a)
    {
        return intExp10(a);
    }
};


/// this one is just for convenience
template <bool B, typename T1, typename T2> using If = std::conditional_t<B, T1, T2>;
/// these ones for better semantics
template <typename T> using Then = T;
template <typename T> using Else = T;

/// Used to indicate undefined operation
struct InvalidType;

template <typename T>
struct DataTypeFromFieldType
{
    using Type = DataTypeNumber<T>;
};

template <>
struct DataTypeFromFieldType<NumberTraits::Error>
{
    using Type = InvalidType;
};

template <typename DataType> constexpr bool IsIntegral = false;
template <> constexpr bool IsIntegral<DataTypeUInt8> = true;
template <> constexpr bool IsIntegral<DataTypeUInt16> = true;
template <> constexpr bool IsIntegral<DataTypeUInt32> = true;
template <> constexpr bool IsIntegral<DataTypeUInt64> = true;
template <> constexpr bool IsIntegral<DataTypeInt8> = true;
template <> constexpr bool IsIntegral<DataTypeInt16> = true;
template <> constexpr bool IsIntegral<DataTypeInt32> = true;
template <> constexpr bool IsIntegral<DataTypeInt64> = true;

template <typename DataType> constexpr bool IsDateOrDateTime = false;
template <> constexpr bool IsDateOrDateTime<DataTypeDate> = true;
template <> constexpr bool IsDateOrDateTime<DataTypeDateTime> = true;

/** Returns appropriate result type for binary operator on dates (or datetimes):
 *  Date + Integral -> Date
 *  Integral + Date -> Date
 *  Date - Date     -> Int32
 *  Date - Integral -> Date
 *  least(Date, Date) -> Date
 *  greatest(Date, Date) -> Date
 *  All other operations are not defined and return InvalidType, operations on
 *  distinct date types are also undefined (e.g. DataTypeDate - DataTypeDateTime)
 */
template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct DateBinaryOperationTraits
{
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using Op = Operation<T0, T1>;

    using ResultDataType =
        If<std::is_same_v<Op, PlusImpl<T0, T1>>,
            Then<
                If<IsDateOrDateTime<LeftDataType> && IsIntegral<RightDataType>,
                    Then<LeftDataType>,
                    Else<
                        If<IsIntegral<LeftDataType> && IsDateOrDateTime<RightDataType>,
                            Then<RightDataType>,
                            Else<InvalidType>>>>>,
            Else<
                If<std::is_same_v<Op, MinusImpl<T0, T1>>,
                    Then<
                        If<IsDateOrDateTime<LeftDataType>,
                            Then<
                                If<std::is_same_v<LeftDataType, RightDataType>,
                                    Then<DataTypeInt32>,
                                    Else<
                                        If<IsIntegral<RightDataType>,
                                            Then<LeftDataType>,
                                            Else<InvalidType>>>>>,
                            Else<InvalidType>>>,
                    Else<
                        If<std::is_same_v<T0, T1>
                            && (std::is_same_v<Op, LeastImpl<T0, T1>> || std::is_same_v<Op, GreatestImpl<T0, T1>>),
                            Then<LeftDataType>,
                            Else<InvalidType>>>>>>;
};


/// Decides among date and numeric operations
template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct BinaryOperationTraits
{
    using ResultDataType =
        If<IsDateOrDateTime<LeftDataType> || IsDateOrDateTime<RightDataType>,
            Then<
                typename DateBinaryOperationTraits<
                    Operation, LeftDataType, RightDataType>::ResultDataType>,
            Else<
                typename DataTypeFromFieldType<
                    typename Operation<
                        typename LeftDataType::FieldType,
                        typename RightDataType::FieldType>::ResultType>::Type>>;
};


template <template <typename, typename> class Op, typename Name>
class FunctionBinaryArithmetic : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBinaryArithmetic>(context); }

    FunctionBinaryArithmetic(const Context & context) : context(context) {}

private:
    const Context & context;

    template <typename ResultDataType>
    bool checkRightTypeImpl(DataTypePtr & type_res) const
    {
        /// Overload for InvalidType
        if constexpr (std::is_same_v<ResultDataType, InvalidType>)
            return false;
        else
        {
            type_res = std::make_shared<ResultDataType>();
            return true;
        }
    }

    template <typename LeftDataType, typename RightDataType>
    bool checkRightType(const DataTypes & arguments, DataTypePtr & type_res) const
    {
        using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

        if (typeid_cast<const RightDataType *>(arguments[1].get()))
            return checkRightTypeImpl<ResultDataType>(type_res);

        return false;
    }

    template <typename T0>
    bool checkLeftType(const DataTypes & arguments, DataTypePtr & type_res) const
    {
        if (typeid_cast<const T0 *>(arguments[0].get()))
        {
            if (   checkRightType<T0, DataTypeDate>(arguments, type_res)
                || checkRightType<T0, DataTypeDateTime>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt8>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt16>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt32>(arguments, type_res)
                || checkRightType<T0, DataTypeUInt64>(arguments, type_res)
                || checkRightType<T0, DataTypeInt8>(arguments, type_res)
                || checkRightType<T0, DataTypeInt16>(arguments, type_res)
                || checkRightType<T0, DataTypeInt32>(arguments, type_res)
                || checkRightType<T0, DataTypeInt64>(arguments, type_res)
                || checkRightType<T0, DataTypeFloat32>(arguments, type_res)
                || checkRightType<T0, DataTypeFloat64>(arguments, type_res))
                return true;
        }
        return false;
    }

    /// Overload for date operations
    template <typename LeftDataType, typename RightDataType, typename ColumnType>
    bool executeRightType(Block & block, const ColumnNumbers & arguments, const size_t result, const ColumnType * col_left)
    {
        if (!typeid_cast<const RightDataType *>(block.getByPosition(arguments[1]).type.get()))
            return false;

        using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

        return executeRightTypeDispatch<LeftDataType, RightDataType, ResultDataType>(
            block, arguments, result, col_left);
    }

    /// Overload for InvalidType
    template <typename LeftDataType, typename RightDataType, typename ResultDataType, typename ColumnType>
    bool executeRightTypeDispatch(Block & block, const ColumnNumbers & arguments,
        [[maybe_unused]] const size_t result, [[maybe_unused]] const ColumnType * col_left)
    {
        if constexpr (std::is_same_v<ResultDataType, InvalidType>)
            throw Exception("Types " + String(TypeName<typename LeftDataType::FieldType>::get())
                + " and " + String(TypeName<typename LeftDataType::FieldType>::get())
                + " are incompatible for function " + getName() + " or not upscaleable to common type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        else
        {
            using T0 = typename LeftDataType::FieldType;
            using T1 = typename RightDataType::FieldType;
            using ResultType = typename ResultDataType::FieldType;

            return executeRightTypeImpl<T0, T1, ResultType>(block, arguments, result, col_left);
        }
    }

    /// ColumnVector overload
    template <typename T0, typename T1, typename ResultType = typename Op<T0, T1>::ResultType>
    bool executeRightTypeImpl(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnVector<T0> * col_left)
    {
        if (auto col_right = checkAndGetColumn<ColumnVector<T1>>(block.getByPosition(arguments[1]).column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            auto & vec_res = col_res->getData();
            vec_res.resize(col_left->getData().size());
            BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::vector_vector(col_left->getData(), col_right->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }
        else if (auto col_right = checkAndGetColumnConst<ColumnVector<T1>>(block.getByPosition(arguments[1]).column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            auto & vec_res = col_res->getData();
            vec_res.resize(col_left->getData().size());
            BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::vector_constant(col_left->getData(), col_right->template getValue<T1>(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }

        throw Exception("Logical error: unexpected type of column", ErrorCodes::LOGICAL_ERROR);
    }

    /// ColumnConst overload
    template <typename T0, typename T1, typename ResultType = typename Op<T0, T1>::ResultType>
    bool executeRightTypeImpl(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnConst * col_left)
    {
        if (auto col_right = checkAndGetColumn<ColumnVector<T1>>(block.getByPosition(arguments[1]).column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            auto & vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::constant_vector(col_left->template getValue<T0>(), col_right->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }
        else if (auto col_right = checkAndGetColumnConst<ColumnVector<T1>>(block.getByPosition(arguments[1]).column.get()))
        {
            ResultType res = 0;
            BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::constant_constant(col_left->template getValue<T0>(), col_right->template getValue<T1>(), res);
            block.getByPosition(result).column = DataTypeNumber<ResultType>().createColumnConst(col_left->size(), toField(res));

            return true;
        }

        return false;
    }

    template <typename LeftDataType>
    bool executeLeftType(Block & block, const ColumnNumbers & arguments, const size_t result)
    {
        if (!typeid_cast<const LeftDataType *>(block.getByPosition(arguments[0]).type.get()))
            return false;

        return executeLeftTypeImpl<LeftDataType>(block, arguments, result);
    }

    template <typename LeftDataType>
    bool executeLeftTypeImpl(Block & block, const ColumnNumbers & arguments, const size_t result)
    {
        if (auto col_left = checkAndGetColumn<ColumnVector<typename LeftDataType::FieldType>>(block.getByPosition(arguments[0]).column.get()))
        {
            if (   executeRightType<LeftDataType, DataTypeDate>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeDateTime>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeUInt8>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeUInt16>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeUInt32>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeUInt64>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeInt8>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeInt16>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeInt32>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeInt64>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeFloat32>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeFloat64>(block, arguments, result, col_left))
                return true;
            else
                throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
                    + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (auto col_left = checkAndGetColumnConst<ColumnVector<typename LeftDataType::FieldType>>(block.getByPosition(arguments[0]).column.get()))
        {
            if (   executeRightType<LeftDataType, DataTypeDate>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeDateTime>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeUInt8>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeUInt16>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeUInt32>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeUInt64>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeInt8>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeInt16>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeInt32>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeInt64>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeFloat32>(block, arguments, result, col_left)
                || executeRightType<LeftDataType, DataTypeFloat64>(block, arguments, result, col_left))
                return true;
            else
                throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
                    + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        return false;
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

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        if (auto function_builder = getFunctionForIntervalArithmetic(arguments[0], arguments[1]))
        {
            ColumnsWithTypeAndName new_arguments(2);

            for (size_t i = 0; i < 2; ++i)
                new_arguments[i].type = arguments[i];

            /// Interval argument must be second.
            if (checkDataType<DataTypeInterval>(new_arguments[0].type.get()))
                std::swap(new_arguments[0], new_arguments[1]);

            /// Change interval argument to its representation
            new_arguments[1].type = std::make_shared<DataTypeNumber<DataTypeInterval::FieldType>>();

            auto function = function_builder->build(new_arguments);
            return function->getReturnType();
        }

        DataTypePtr type_res;

        if (!( checkLeftType<DataTypeDate>(arguments, type_res)
            || checkLeftType<DataTypeDateTime>(arguments, type_res)
            || checkLeftType<DataTypeUInt8>(arguments, type_res)
            || checkLeftType<DataTypeUInt16>(arguments, type_res)
            || checkLeftType<DataTypeUInt32>(arguments, type_res)
            || checkLeftType<DataTypeUInt64>(arguments, type_res)
            || checkLeftType<DataTypeInt8>(arguments, type_res)
            || checkLeftType<DataTypeInt16>(arguments, type_res)
            || checkLeftType<DataTypeInt32>(arguments, type_res)
            || checkLeftType<DataTypeInt64>(arguments, type_res)
            || checkLeftType<DataTypeFloat32>(arguments, type_res)
            || checkLeftType<DataTypeFloat64>(arguments, type_res)))
            throw Exception("Illegal types " + arguments[0]->getName() + " and " + arguments[1]->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type_res;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        /// Special case when the function is plus or minus, one of arguments is Date/DateTime and another is Interval.
        if (auto function_builder = getFunctionForIntervalArithmetic(block.getByPosition(arguments[0]).type, block.getByPosition(arguments[1]).type))
        {
            ColumnNumbers new_arguments = arguments;

            /// Interval argument must be second.
            if (checkDataType<DataTypeInterval>(block.getByPosition(arguments[0]).type.get()))
                std::swap(new_arguments[0], new_arguments[1]);

            /// Change interval argument type to its representation
            Block new_block = block;
            new_block.getByPosition(new_arguments[1]).type = std::make_shared<DataTypeNumber<DataTypeInterval::FieldType>>();

            ColumnsWithTypeAndName new_arguments_with_type_and_name =
                    {new_block.getByPosition(new_arguments[0]), new_block.getByPosition(new_arguments[1])};
            auto function = function_builder->build(new_arguments_with_type_and_name);

            function->execute(new_block, new_arguments, result);
            block.getByPosition(result).column = new_block.getByPosition(result).column;

            return;
        }

        if (!( executeLeftType<DataTypeDate>(block, arguments, result)
            || executeLeftType<DataTypeDateTime>(block, arguments, result)
            || executeLeftType<DataTypeUInt8>(block, arguments, result)
            || executeLeftType<DataTypeUInt16>(block, arguments, result)
            || executeLeftType<DataTypeUInt32>(block, arguments, result)
            || executeLeftType<DataTypeUInt64>(block, arguments, result)
            || executeLeftType<DataTypeInt8>(block, arguments, result)
            || executeLeftType<DataTypeInt16>(block, arguments, result)
            || executeLeftType<DataTypeInt32>(block, arguments, result)
            || executeLeftType<DataTypeInt64>(block, arguments, result)
            || executeLeftType<DataTypeFloat32>(block, arguments, result)
            || executeLeftType<DataTypeFloat64>(block, arguments, result)))
           throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <typename FunctionName>
struct FunctionUnaryArithmeticMonotonicity;


template <template <typename> class Op, typename Name, bool is_injective>
class FunctionUnaryArithmetic : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUnaryArithmetic>(); }

private:
    template <typename T0>
    bool checkType(const DataTypes & arguments, DataTypePtr & result) const
    {
        if (typeid_cast<const T0 *>(arguments[0].get()))
        {
            result = std::make_shared<DataTypeNumber<typename Op<typename T0::FieldType>::ResultType>>();
            return true;
        }
        return false;
    }

    template <typename T0>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnVector<T0> * col = checkAndGetColumn<ColumnVector<T0>>(block.getByPosition(arguments[0]).column.get()))
        {
            using ResultType = typename Op<T0>::ResultType;

            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->getData().size());
            UnaryOperationImpl<T0, Op<T0>>::vector(col->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }

        return false;
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block &) override { return is_injective; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr result;

        if (!( checkType<DataTypeUInt8>(arguments, result)
            || checkType<DataTypeUInt16>(arguments, result)
            || checkType<DataTypeUInt32>(arguments, result)
            || checkType<DataTypeUInt64>(arguments, result)
            || checkType<DataTypeInt8>(arguments, result)
            || checkType<DataTypeInt16>(arguments, result)
            || checkType<DataTypeInt32>(arguments, result)
            || checkType<DataTypeInt64>(arguments, result)
            || checkType<DataTypeFloat32>(arguments, result)
            || checkType<DataTypeFloat64>(arguments, result)))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return result;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!( executeType<UInt8>(block, arguments, result)
            || executeType<UInt16>(block, arguments, result)
            || executeType<UInt32>(block, arguments, result)
            || executeType<UInt64>(block, arguments, result)
            || executeType<Int8>(block, arguments, result)
            || executeType<Int16>(block, arguments, result)
            || executeType<Int32>(block, arguments, result)
            || executeType<Int64>(block, arguments, result)
            || executeType<Float32>(block, arguments, result)
            || executeType<Float64>(block, arguments, result)))
           throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return FunctionUnaryArithmeticMonotonicity<Name>::has();
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field & left, const Field & right) const override
    {
        return FunctionUnaryArithmeticMonotonicity<Name>::get(left, right);
    }
};


struct NamePlus                 { static constexpr auto name = "plus"; };
struct NameMinus                { static constexpr auto name = "minus"; };
struct NameMultiply             { static constexpr auto name = "multiply"; };
struct NameDivideFloating       { static constexpr auto name = "divide"; };
struct NameDivideIntegral       { static constexpr auto name = "intDiv"; };
struct NameDivideIntegralOrZero { static constexpr auto name = "intDivOrZero"; };
struct NameModulo               { static constexpr auto name = "modulo"; };
struct NameNegate               { static constexpr auto name = "negate"; };
struct NameAbs                  { static constexpr auto name = "abs"; };
struct NameBitAnd               { static constexpr auto name = "bitAnd"; };
struct NameBitOr                { static constexpr auto name = "bitOr"; };
struct NameBitXor               { static constexpr auto name = "bitXor"; };
struct NameBitNot               { static constexpr auto name = "bitNot"; };
struct NameBitShiftLeft         { static constexpr auto name = "bitShiftLeft"; };
struct NameBitShiftRight        { static constexpr auto name = "bitShiftRight"; };
struct NameBitRotateLeft        { static constexpr auto name = "bitRotateLeft"; };
struct NameBitRotateRight       { static constexpr auto name = "bitRotateRight"; };
struct NameBitTest              { static constexpr auto name = "bitTest"; };
struct NameBitTestAny           { static constexpr auto name = "bitTestAny"; };
struct NameBitTestAll           { static constexpr auto name = "bitTestAll"; };
struct NameLeast                { static constexpr auto name = "least"; };
struct NameGreatest             { static constexpr auto name = "greatest"; };
struct NameGCD                  { static constexpr auto name = "gcd"; };
struct NameLCM                  { static constexpr auto name = "lcm"; };
struct NameIntExp2              { static constexpr auto name = "intExp2"; };
struct NameIntExp10             { static constexpr auto name = "intExp10"; };

using FunctionPlus = FunctionBinaryArithmetic<PlusImpl, NamePlus>;
using FunctionMinus = FunctionBinaryArithmetic<MinusImpl, NameMinus>;
using FunctionMultiply = FunctionBinaryArithmetic<MultiplyImpl, NameMultiply>;
using FunctionDivideFloating = FunctionBinaryArithmetic<DivideFloatingImpl, NameDivideFloating>;
using FunctionDivideIntegral = FunctionBinaryArithmetic<DivideIntegralImpl, NameDivideIntegral>;
using FunctionDivideIntegralOrZero = FunctionBinaryArithmetic<DivideIntegralOrZeroImpl, NameDivideIntegralOrZero>;
using FunctionModulo = FunctionBinaryArithmetic<ModuloImpl, NameModulo>;
using FunctionNegate = FunctionUnaryArithmetic<NegateImpl, NameNegate, true>;
using FunctionAbs = FunctionUnaryArithmetic<AbsImpl, NameAbs, false>;
using FunctionBitAnd = FunctionBinaryArithmetic<BitAndImpl, NameBitAnd>;
using FunctionBitOr = FunctionBinaryArithmetic<BitOrImpl, NameBitOr>;
using FunctionBitXor = FunctionBinaryArithmetic<BitXorImpl, NameBitXor>;
using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot, true>;
using FunctionBitShiftLeft = FunctionBinaryArithmetic<BitShiftLeftImpl, NameBitShiftLeft>;
using FunctionBitShiftRight = FunctionBinaryArithmetic<BitShiftRightImpl, NameBitShiftRight>;
using FunctionBitRotateLeft = FunctionBinaryArithmetic<BitRotateLeftImpl, NameBitRotateLeft>;
using FunctionBitRotateRight = FunctionBinaryArithmetic<BitRotateRightImpl, NameBitRotateRight>;
using FunctionBitTest = FunctionBinaryArithmetic<BitTestImpl, NameBitTest>;
using FunctionLeast = FunctionBinaryArithmetic<LeastImpl, NameLeast>;
using FunctionGreatest = FunctionBinaryArithmetic<GreatestImpl, NameGreatest>;
using FunctionGCD = FunctionBinaryArithmetic<GCDImpl, NameGCD>;
using FunctionLCM = FunctionBinaryArithmetic<LCMImpl, NameLCM>;
/// Assumed to be injective for the purpose of query optimization, but in fact it is not injective because of possible overflow.
using FunctionIntExp2 = FunctionUnaryArithmetic<IntExp2Impl, NameIntExp2, true>;
using FunctionIntExp10 = FunctionUnaryArithmetic<IntExp10Impl, NameIntExp10, true>;


/// Monotonicity properties for some functions.

template <> struct FunctionUnaryArithmeticMonotonicity<NameNegate>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return { true, false };
    }
};

template <> struct FunctionUnaryArithmeticMonotonicity<NameAbs>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if ((left_float < 0 && right_float > 0) || (left_float > 0 && right_float < 0))
            return {};

        return { true, (left_float > 0) };
    }
};

template <> struct FunctionUnaryArithmeticMonotonicity<NameBitNot>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const Field &, const Field &)
    {
        return {};
    }
};

template <> struct FunctionUnaryArithmeticMonotonicity<NameIntExp2>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 63)
            return {};

        return { true };
    }
};

template <> struct FunctionUnaryArithmeticMonotonicity<NameIntExp10>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);

        if (left_float < 0 || right_float > 19)
            return {};

        return { true };
    }
};

}

/// Optimizations for integer division by a constant.

#if __SSE2__
    #define LIBDIVIDE_USE_SSE2 1
#endif

#include <libdivide.h>

namespace DB
{

template <typename A, typename B>
struct DivideIntegralByConstantImpl
    : BinaryOperationImplBase<A, B, DivideIntegralImpl<A, B>>
{
    using ResultType = typename DivideIntegralImpl<A, B>::ResultType;

    static void vector_constant(const PaddedPODArray<A> & a, B b, PaddedPODArray<ResultType> & c)
    {
        if (unlikely(b == 0))
            throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

        if (unlikely(std::is_signed_v<B> && b == -1))
        {
            size_t size = a.size();
            for (size_t i = 0; i < size; ++i)
                c[i] = -c[i];
            return;
        }

#pragma GCC diagnostic pop

        libdivide::divider<A> divider(b);

        size_t size = a.size();
        const A * a_pos = &a[0];
        const A * a_end = a_pos + size;
        ResultType * c_pos = &c[0];

#if __SSE2__
        static constexpr size_t values_per_sse_register = 16 / sizeof(A);
        const A * a_end_sse = a_pos + size / values_per_sse_register * values_per_sse_register;

        while (a_pos < a_end_sse)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(c_pos),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(a_pos)) / divider);

            a_pos += values_per_sse_register;
            c_pos += values_per_sse_register;
        }
#endif

        while (a_pos < a_end)
        {
            *c_pos = *a_pos / divider;
            ++a_pos;
            ++c_pos;
        }
    }
};

template <typename A, typename B>
struct ModuloByConstantImpl
    : BinaryOperationImplBase<A, B, ModuloImpl<A, B>>
{
    using ResultType = typename ModuloImpl<A, B>::ResultType;

    static void vector_constant(const PaddedPODArray<A> & a, B b, PaddedPODArray<ResultType> & c)
    {
        if (unlikely(b == 0))
            throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

        if (unlikely((std::is_signed_v<B> && b == -1) || b == 1))
        {
            size_t size = a.size();
            for (size_t i = 0; i < size; ++i)
                c[i] = 0;
            return;
        }

#pragma GCC diagnostic pop

        libdivide::divider<A> divider(b);

        /// Here we failed to make the SSE variant from libdivide give an advantage.
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = a[i] - (a[i] / divider) * b; /// NOTE: perhaps, the division semantics with the remainder of negative numbers is not preserved.
    }
};


/** Specializations are specified for dividing numbers of the type UInt64 and UInt32 by the numbers of the same sign.
  * Can be expanded to all possible combinations, but more code is needed.
  */

template <> struct BinaryOperationImpl<UInt64, UInt8, DivideIntegralImpl<UInt64, UInt8>> : DivideIntegralByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16, DivideIntegralImpl<UInt64, UInt16>> : DivideIntegralByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32, DivideIntegralImpl<UInt64, UInt32>> : DivideIntegralByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64, DivideIntegralImpl<UInt64, UInt64>> : DivideIntegralByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8, DivideIntegralImpl<UInt32, UInt8>> : DivideIntegralByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16, DivideIntegralImpl<UInt32, UInt16>> : DivideIntegralByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32, DivideIntegralImpl<UInt32, UInt32>> : DivideIntegralByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64, DivideIntegralImpl<UInt32, UInt64>> : DivideIntegralByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8, DivideIntegralImpl<Int64, Int8>> : DivideIntegralByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16, DivideIntegralImpl<Int64, Int16>> : DivideIntegralByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32, DivideIntegralImpl<Int64, Int32>> : DivideIntegralByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64, DivideIntegralImpl<Int64, Int64>> : DivideIntegralByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8, DivideIntegralImpl<Int32, Int8>> : DivideIntegralByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16, DivideIntegralImpl<Int32, Int16>> : DivideIntegralByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32, DivideIntegralImpl<Int32, Int32>> : DivideIntegralByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64, DivideIntegralImpl<Int32, Int64>> : DivideIntegralByConstantImpl<Int32, Int64> {};


template <> struct BinaryOperationImpl<UInt64, UInt8, ModuloImpl<UInt64, UInt8>> : ModuloByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16, ModuloImpl<UInt64, UInt16>> : ModuloByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32, ModuloImpl<UInt64, UInt32>> : ModuloByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64, ModuloImpl<UInt64, UInt64>> : ModuloByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8, ModuloImpl<UInt32, UInt8>> : ModuloByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16, ModuloImpl<UInt32, UInt16>> : ModuloByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32, ModuloImpl<UInt32, UInt32>> : ModuloByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64, ModuloImpl<UInt32, UInt64>> : ModuloByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8, ModuloImpl<Int64, Int8>> : ModuloByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16, ModuloImpl<Int64, Int16>> : ModuloByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32, ModuloImpl<Int64, Int32>> : ModuloByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64, ModuloImpl<Int64, Int64>> : ModuloByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8, ModuloImpl<Int32, Int8>> : ModuloByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16, ModuloImpl<Int32, Int16>> : ModuloByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32, ModuloImpl<Int32, Int32>> : ModuloByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64, ModuloImpl<Int32, Int64>> : ModuloByConstantImpl<Int32, Int64> {};


template <typename Impl, typename Name>
struct FunctionBitTestMany : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitTestMany>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception{
                "Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 2.",
                ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION};

        const auto first_arg = arguments.front().get();

        if (!first_arg->isInteger())
            throw Exception{
                "Illegal type " + first_arg->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};


        for (const auto i : ext::range(1, arguments.size()))
        {
            const auto pos_arg = arguments[i].get();

            if (!pos_arg->isUnsignedInteger())
                throw Exception{
                    "Illegal type " + pos_arg->getName() + " of " + toString(i) + " argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto value_col = block.getByPosition(arguments.front()).column.get();

        if (!execute<UInt8>(block, arguments, result, value_col)
            && !execute<UInt16>(block, arguments, result, value_col)
            && !execute<UInt32>(block, arguments, result, value_col)
            && !execute<UInt64>(block, arguments, result, value_col)
            && !execute<Int8>(block, arguments, result, value_col)
            && !execute<Int16>(block, arguments, result, value_col)
            && !execute<Int32>(block, arguments, result, value_col)
            && !execute<Int64>(block, arguments, result, value_col))
            throw Exception{
                "Illegal column " + value_col->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
    }

private:
    template <typename T>
    bool execute(
        Block & block, const ColumnNumbers & arguments, const size_t result,
        const IColumn * const value_col_untyped)
    {
        if (const auto value_col = checkAndGetColumn<ColumnVector<T>>(value_col_untyped))
        {
            const auto size = value_col->size();
            bool is_const;
            const auto mask = createConstMask<T>(block, arguments, is_const);
            const auto & val = value_col->getData();

            auto out_col = ColumnVector<UInt8>::create(size);
            auto & out = out_col->getData();

            if (is_const)
            {
                for (const auto i : ext::range(0, size))
                    out[i] = Impl::apply(val[i], mask);
            }
            else
            {
                const auto mask = createMask<T>(size, block, arguments);

                for (const auto i : ext::range(0, size))
                    out[i] = Impl::apply(val[i], mask[i]);
            }

            block.getByPosition(result).column = std::move(out_col);
            return true;
        }
        else if (const auto value_col = checkAndGetColumnConst<ColumnVector<T>>(value_col_untyped))
        {
            const auto size = value_col->size();
            bool is_const;
            const auto mask = createConstMask<T>(block, arguments, is_const);
            const auto val = value_col->template getValue<T>();

            if (is_const)
            {
                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(size, toField(Impl::apply(val, mask)));
            }
            else
            {
                const auto mask = createMask<T>(size, block, arguments);
                auto out_col = ColumnVector<UInt8>::create(size);

                auto & out = out_col->getData();

                for (const auto i : ext::range(0, size))
                    out[i] = Impl::apply(val, mask[i]);

                block.getByPosition(result).column = std::move(out_col);
            }

            return true;
        }

        return false;
    }

    template <typename ValueType>
    ValueType createConstMask(const Block & block, const ColumnNumbers & arguments, bool & is_const)
    {
        is_const = true;
        ValueType mask = 0;

        for (const auto i : ext::range(1, arguments.size()))
        {
            if (auto pos_col_const = checkAndGetColumnConst<ColumnVector<ValueType>>(block.getByPosition(arguments[i]).column.get()))
            {
                const auto pos = pos_col_const->template getValue<ValueType>();
                mask = mask | (1 << pos);
            }
            else
            {
                is_const = false;
                return {};
            }
        }

        return mask;
    }

    template <typename ValueType>
    PaddedPODArray<ValueType> createMask(const size_t size, const Block & block, const ColumnNumbers & arguments)
    {
        PaddedPODArray<ValueType> mask(size, ValueType{});

        for (const auto i : ext::range(1, arguments.size()))
        {
            const auto pos_col = block.getByPosition(arguments[i]).column.get();

            if (!addToMaskImpl<UInt8>(mask, pos_col)
                && !addToMaskImpl<UInt16>(mask, pos_col)
                && !addToMaskImpl<UInt32>(mask, pos_col)
                && !addToMaskImpl<UInt64>(mask, pos_col))
                throw Exception{
                    "Illegal column " + pos_col->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN};
        }

        return mask;
    }

    template <typename PosType, typename ValueType>
    bool addToMaskImpl(PaddedPODArray<ValueType> & mask, const IColumn * const pos_col_untyped)
    {
        if (const auto pos_col = checkAndGetColumn<ColumnVector<PosType>>(pos_col_untyped))
        {
            const auto & pos = pos_col->getData();

            for (const auto i : ext::range(0, mask.size()))
                mask[i] = mask[i] | (1 << pos[i]);

            return true;
        }
        else if (const auto pos_col = checkAndGetColumnConst<ColumnVector<PosType>>(pos_col_untyped))
        {
            const auto & pos = pos_col->template getValue<PosType>();
            const auto new_mask = 1 << pos;

            for (const auto i : ext::range(0, mask.size()))
                mask[i] = mask[i] | new_mask;

            return true;
        }

        return false;
    }
};


struct BitTestAnyImpl
{
    template <typename A, typename B>
    static inline UInt8 apply(A a, B b) { return (a & b) != 0; };
};

struct BitTestAllImpl
{
    template <typename A, typename B>
    static inline UInt8 apply(A a, B b) { return (a & b) == b; };
};


using FunctionBitTestAny = FunctionBitTestMany<BitTestAnyImpl, NameBitTestAny>;
using FunctionBitTestAll = FunctionBitTestMany<BitTestAllImpl, NameBitTestAll>;

}
