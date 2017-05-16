#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Functions/NumberTraits.h>
#include <Functions/AccurateComparison.h>
#include <Core/FieldVisitors.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
}


/** Arithmetic operations: +, -, *, /, %,
  * intDiv (integer division), unary minus.
  * Bitwise operations: |, &, ^, ~.
  * Etc.
  */

template<typename A, typename B, typename Op, typename ResultType_ = typename Op::ResultType>
struct BinaryOperationImplBase
{
    using ResultType = ResultType_;

    static void vector_vector(const PaddedPODArray<A> & a, const PaddedPODArray<B> & b, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::template apply<ResultType>(a[i], b[i]);
    }

    static void vector_constant(const PaddedPODArray<A> & a, B b, PaddedPODArray<ResultType> & c)
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = Op::template apply<ResultType>(a[i], b);
    }

    static void constant_vector(A a, const PaddedPODArray<B> & b, PaddedPODArray<ResultType> & c)
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

template<typename A, typename B, typename Op, typename ResultType = typename Op::ResultType>
struct BinaryOperationImpl : BinaryOperationImplBase<A, B, Op, ResultType>
{
};


template<typename A, typename Op>
struct UnaryOperationImpl
{
    using ResultType = typename Op::ResultType;

    static void vector(const PaddedPODArray<A> & a, PaddedPODArray<ResultType> & c)
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


template<typename A, typename B>
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


template<typename A, typename B>
struct MultiplyImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) * b;
    }
};

template<typename A, typename B>
struct MinusImpl
{
    using ResultType = typename NumberTraits::ResultOfSubtraction<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) - b;
    }
};

template<typename A, typename B>
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
    if (unlikely(std::is_signed<A>::value && std::is_signed<B>::value && a == std::numeric_limits<A>::min() && b == -1))
        throw Exception("Division of minimal signed number by minus one", ErrorCodes::ILLEGAL_DIVISION);
}

template <typename A, typename B>
inline bool divisionLeadsToFPE(A a, B b)
{
    /// Is it better to use siglongjmp instead of checks?

    if (unlikely(b == 0))
        return true;

    /// http://avva.livejournal.com/2548306.html
    if (unlikely(std::is_signed<A>::value && std::is_signed<B>::value && a == std::numeric_limits<A>::min() && b == -1))
        return true;

    return false;
}


#pragma GCC diagnostic pop


template<typename A, typename B>
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

template<typename A, typename B>
struct DivideIntegralOrZeroImpl
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return unlikely(divisionLeadsToFPE(a, b)) ? 0 : a / b;
    }
};

template<typename A, typename B>
struct ModuloImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<A>::Type(b));
        return typename NumberTraits::ToInteger<A>::Type(a)
            % typename NumberTraits::ToInteger<A>::Type(b);
    }
};

template<typename A, typename B>
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

template<typename A, typename B>
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

template<typename A, typename B>
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

template<typename A, typename B>
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

template<typename A, typename B>
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

template<typename A, typename B>
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

template<typename A, typename B>
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


template<typename A, typename B>
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

template<typename A, typename B>
struct LeastSpecialImpl
{
    using ResultType = std::make_signed_t<A>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        static_assert(std::is_same<Result, ResultType>::value, "ResultType != Result");
        return accurate::lessOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
};

template<typename A, typename B>
using LeastImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>::value, LeastBaseImpl<A, B>, LeastSpecialImpl<A, B>>;


template<typename A, typename B>
struct GreatestBaseImpl
{
    using ResultType = NumberTraits::ResultOfGreatest<A, B>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) > static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
};

template<typename A, typename B>
struct GreatestSpecialImpl
{
    using ResultType = std::make_unsigned_t<A>;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        static_assert(std::is_same<Result, ResultType>::value, "ResultType != Result");
        return accurate::greaterOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
};

template<typename A, typename B>
using GreatestImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>::value, GreatestBaseImpl<A, B>, GreatestSpecialImpl<A, B>>;


template<typename A>
struct NegateImpl
{
    using ResultType = typename NumberTraits::ResultOfNegate<A>::Type;

    static inline ResultType apply(A a)
    {
        return -static_cast<ResultType>(a);
    }
};

template<typename A>
struct BitNotImpl
{
    using ResultType = typename NumberTraits::ResultOfBitNot<A>::Type;

    static inline ResultType apply(A a)
    {
        return ~static_cast<ResultType>(a);
    }
};

template<typename A>
struct AbsImpl
{
    using ResultType = typename NumberTraits::ResultOfAbs<A>::Type;

    template<typename T = A>
    static inline ResultType apply(T a,
        typename std::enable_if<std::is_integral<T>::value && std::is_signed<T>::value, void>::type * = nullptr)
    {
        return a < 0 ? static_cast<ResultType>(~a) + 1 : a;
    }

    template<typename T = A>
    static inline ResultType apply(T a,
        typename std::enable_if<std::is_integral<T>::value && std::is_unsigned<T>::value, void>::type * = nullptr)
    {
        return static_cast<ResultType>(a);
    }

    template<typename T = A>
    static inline ResultType apply(T a, typename std::enable_if<std::is_floating_point<T>::value, void>::type * = nullptr)
    {
        return static_cast<ResultType>(std::abs(a));
    }
};

/// this one is just for convenience
template <bool B, typename T1, typename T2> using If = typename std::conditional<B, T1, T2>::type;
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

template <typename DataType> struct IsIntegral { static constexpr auto value = false; };
template <> struct IsIntegral<DataTypeUInt8> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeUInt16> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeUInt32> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeUInt64> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeInt8> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeInt16> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeInt32> { static constexpr auto value = true; };
template <> struct IsIntegral<DataTypeInt64> { static constexpr auto value = true; };

template <typename DataType> struct IsFloating { static constexpr auto value = false; };
template <> struct IsFloating<DataTypeFloat32> { static constexpr auto value = true; };
template <> struct IsFloating<DataTypeFloat64> { static constexpr auto value = true; };

template <typename DataType> struct IsNumeric
{
    static constexpr auto value = IsIntegral<DataType>::value || IsFloating<DataType>::value;
};

template <typename DataType> struct IsDateOrDateTime { static constexpr auto value = false; };
template <> struct IsDateOrDateTime<DataTypeDate> { static constexpr auto value = true; };
template <> struct IsDateOrDateTime<DataTypeDateTime> { static constexpr auto value = true; };

/** Returns appropriate result type for binary operator on dates (or datetimes):
 *  Date + Integral -> Date
 *  Integral + Date -> Date
 *  Date - Date     -> Int32
 *  Date - Integral -> Date
 *  least(Date, Date) -> Date
 *  greatest(Date, Date) -> Date
 *  All other operations are not defined and return InvalidType, operations on
 *  distinct date types are also undefined (e.g. DataTypeDate - DataTypeDateTime) */
template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct DateBinaryOperationTraits
{
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using Op = Operation<T0, T1>;

    using ResultDataType =
        If<std::is_same<Op, PlusImpl<T0, T1>>::value,
            Then<
                If<IsDateOrDateTime<LeftDataType>::value && IsIntegral<RightDataType>::value,
                    Then<LeftDataType>,
                    Else<
                        If<IsIntegral<LeftDataType>::value && IsDateOrDateTime<RightDataType>::value,
                            Then<RightDataType>,
                            Else<InvalidType>
                        >
                    >
                >
            >,
            Else<
                If<std::is_same<Op, MinusImpl<T0, T1>>::value,
                    Then<
                        If<IsDateOrDateTime<LeftDataType>::value,
                            Then<
                                If<std::is_same<LeftDataType, RightDataType>::value,
                                    Then<DataTypeInt32>,
                                    Else<
                                        If<IsIntegral<RightDataType>::value,
                                            Then<LeftDataType>,
                                            Else<InvalidType>
                                        >
                                    >
                                >
                            >,
                            Else<InvalidType>
                        >
                    >,
                    Else<
                        If<std::is_same<T0, T1>::value
                            && (std::is_same<Op, LeastImpl<T0, T1>>::value || std::is_same<Op, GreatestImpl<T0, T1>>::value),
                            Then<LeftDataType>,
                            Else<InvalidType>
                        >
                    >
                >
            >
        >;
};


/// Decides among date and numeric operations
template <template <typename, typename> class Operation, typename LeftDataType, typename RightDataType>
struct BinaryOperationTraits
{
    using ResultDataType =
        If<IsDateOrDateTime<LeftDataType>::value || IsDateOrDateTime<RightDataType>::value,
            Then<
                typename DateBinaryOperationTraits<
                    Operation, LeftDataType, RightDataType
                >::ResultDataType
            >,
            Else<
                typename DataTypeFromFieldType<
                    typename Operation<
                        typename LeftDataType::FieldType,
                        typename RightDataType::FieldType
                    >::ResultType
                >::Type
            >
        >;
};


template <template <typename, typename> class Op, typename Name>
class FunctionBinaryArithmetic : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBinaryArithmetic>(); }

private:
    /// Overload for InvalidType
    template <typename ResultDataType,
              typename std::enable_if<std::is_same<ResultDataType, InvalidType>::value>::type * = nullptr>
    bool checkRightTypeImpl(DataTypePtr & type_res) const
    {
        return false;
    }

    /// Overload for well-defined operations
    template <typename ResultDataType,
              typename std::enable_if<!std::is_same<ResultDataType, InvalidType>::value>::type * = nullptr>
    bool checkRightTypeImpl(DataTypePtr & type_res) const
    {
        type_res = std::make_shared<ResultDataType>();
        return true;
    }

    template <typename LeftDataType, typename RightDataType>
    bool checkRightType(const DataTypes & arguments, DataTypePtr & type_res) const
    {
        using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

        if (typeid_cast<const RightDataType *>(&*arguments[1]))
            return checkRightTypeImpl<ResultDataType>(type_res);

        return false;
    }

    template <typename T0>
    bool checkLeftType(const DataTypes & arguments, DataTypePtr & type_res) const
    {
        if (typeid_cast<const T0 *>(&*arguments[0]))
        {
            if (    checkRightType<T0, DataTypeDate>(arguments, type_res)
                ||  checkRightType<T0, DataTypeDateTime>(arguments, type_res)
                ||    checkRightType<T0, DataTypeUInt8>(arguments, type_res)
                ||    checkRightType<T0, DataTypeUInt16>(arguments, type_res)
                ||    checkRightType<T0, DataTypeUInt32>(arguments, type_res)
                ||    checkRightType<T0, DataTypeUInt64>(arguments, type_res)
                ||    checkRightType<T0, DataTypeInt8>(arguments, type_res)
                ||    checkRightType<T0, DataTypeInt16>(arguments, type_res)
                ||    checkRightType<T0, DataTypeInt32>(arguments, type_res)
                ||    checkRightType<T0, DataTypeInt64>(arguments, type_res)
                ||    checkRightType<T0, DataTypeFloat32>(arguments, type_res)
                ||    checkRightType<T0, DataTypeFloat64>(arguments, type_res))
                return true;
            else
                throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        return false;
    }

    /// Overload for date operations
    template <typename LeftDataType, typename RightDataType, typename ColumnType>
    bool executeRightType(Block & block, const ColumnNumbers & arguments, const size_t result, const ColumnType * col_left)
    {
        if (!typeid_cast<const RightDataType *>(block.safeGetByPosition(arguments[1]).type.get()))
            return false;

        using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;

        return executeRightTypeDispatch<LeftDataType, RightDataType, ResultDataType>(
            block, arguments, result, col_left);
    }

    /// Overload for InvalidType
    template <typename LeftDataType, typename RightDataType, typename ResultDataType, typename ColumnType,
              typename std::enable_if<std::is_same<ResultDataType, InvalidType>::value>::type * = nullptr>
    bool executeRightTypeDispatch(Block & block, const ColumnNumbers & arguments, const size_t result,
                                  const ColumnType * col_left)
    {
        throw Exception("Types " + TypeName<typename LeftDataType::FieldType>::get()
            + " and " + TypeName<typename LeftDataType::FieldType>::get()
            + " are incompatible for function " + getName() + " or not upscaleable to common type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// Overload for well-defined operations
    template <typename LeftDataType, typename RightDataType, typename ResultDataType, typename ColumnType,
              typename std::enable_if<!std::is_same<ResultDataType, InvalidType>::value>::type * = nullptr>
    bool executeRightTypeDispatch(Block & block, const ColumnNumbers & arguments, const size_t result,
                                  const ColumnType * col_left)
    {
        using T0 = typename LeftDataType::FieldType;
        using T1 = typename RightDataType::FieldType;
        using ResultType = typename ResultDataType::FieldType;

        return executeRightTypeImpl<T0, T1, ResultType>(block, arguments, result, col_left);
    }

    /// ColumnVector overload
    template <typename T0, typename T1, typename ResultType = typename Op<T0, T1>::ResultType>
    bool executeRightTypeImpl(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnVector<T0> * col_left)
    {
        if (auto col_right = typeid_cast<const ColumnVector<T1> *>(block.safeGetByPosition(arguments[1]).column.get()))
        {
            auto col_res = std::make_shared<ColumnVector<ResultType>>();
            block.safeGetByPosition(result).column = col_res;

            auto & vec_res = col_res->getData();
            vec_res.resize(col_left->getData().size());
            BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::vector_vector(col_left->getData(), col_right->getData(), vec_res);

            return true;
        }
        else if (auto col_right = typeid_cast<const ColumnConst<T1> *>(block.safeGetByPosition(arguments[1]).column.get()))
        {
            auto col_res = std::make_shared<ColumnVector<ResultType>>();
            block.safeGetByPosition(result).column = col_res;

            auto & vec_res = col_res->getData();
            vec_res.resize(col_left->getData().size());
            BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::vector_constant(col_left->getData(), col_right->getData(), vec_res);

            return true;
        }

        throw Exception("Logical error: unexpected type of column", ErrorCodes::LOGICAL_ERROR);
    }

    /// ColumnConst overload
    template <typename T0, typename T1, typename ResultType = typename Op<T0, T1>::ResultType>
    bool executeRightTypeImpl(Block & block, const ColumnNumbers & arguments, size_t result, const ColumnConst<T0> * col_left)
    {
        if (auto col_right = typeid_cast<const ColumnVector<T1> *>(block.safeGetByPosition(arguments[1]).column.get()))
        {
            auto col_res = std::make_shared<ColumnVector<ResultType>>();
            block.safeGetByPosition(result).column = col_res;

            auto & vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::constant_vector(col_left->getData(), col_right->getData(), vec_res);

            return true;
        }
        else if (auto col_right = typeid_cast<const ColumnConst<T1> *>(block.safeGetByPosition(arguments[1]).column.get()))
        {
            ResultType res = 0;
            BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>::constant_constant(col_left->getData(), col_right->getData(), res);

            auto col_res = std::make_shared<ColumnConst<ResultType>>(col_left->size(), res);
            block.safeGetByPosition(result).column = col_res;

            return true;
        }

        return false;
    }

    template <typename LeftDataType>
    bool executeLeftType(Block & block, const ColumnNumbers & arguments, const size_t result)
    {
        if (!typeid_cast<const LeftDataType *>(block.safeGetByPosition(arguments[0]).type.get()))
            return false;

        using T0 = typename LeftDataType::FieldType;

        if (    executeLeftTypeImpl<LeftDataType, ColumnVector<T0>>(block, arguments, result)
            ||    executeLeftTypeImpl<LeftDataType, ColumnConst<T0>>(block, arguments, result))
            return true;

        return false;
    }

    template <typename LeftDataType, typename ColumnType>
    bool executeLeftTypeImpl(Block & block, const ColumnNumbers & arguments, const size_t result)
    {
        if (auto col_left = typeid_cast<const ColumnType *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            if (    executeRightType<LeftDataType, DataTypeDate>(block, arguments, result, col_left)
                ||  executeRightType<LeftDataType, DataTypeDateTime>(block, arguments, result, col_left)
                ||    executeRightType<LeftDataType, DataTypeUInt8>(block, arguments, result, col_left)
                ||    executeRightType<LeftDataType, DataTypeUInt16>(block, arguments, result, col_left)
                ||    executeRightType<LeftDataType, DataTypeUInt32>(block, arguments, result, col_left)
                ||    executeRightType<LeftDataType, DataTypeUInt64>(block, arguments, result, col_left)
                ||    executeRightType<LeftDataType, DataTypeInt8>(block, arguments, result, col_left)
                ||    executeRightType<LeftDataType, DataTypeInt16>(block, arguments, result, col_left)
                ||    executeRightType<LeftDataType, DataTypeInt32>(block, arguments, result, col_left)
                ||    executeRightType<LeftDataType, DataTypeInt64>(block, arguments, result, col_left)
                ||    executeRightType<LeftDataType, DataTypeFloat32>(block, arguments, result, col_left)
                ||    executeRightType<LeftDataType, DataTypeFloat64>(block, arguments, result, col_left))
                return true;
            else
                throw Exception("Illegal column " + block.safeGetByPosition(arguments[1]).column->getName()
                    + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        return false;
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr type_res;

        if (!(    checkLeftType<DataTypeDate>(arguments, type_res)
            ||    checkLeftType<DataTypeDateTime>(arguments, type_res)
            ||    checkLeftType<DataTypeUInt8>(arguments, type_res)
            ||    checkLeftType<DataTypeUInt16>(arguments, type_res)
            ||    checkLeftType<DataTypeUInt32>(arguments, type_res)
            ||    checkLeftType<DataTypeUInt64>(arguments, type_res)
            ||    checkLeftType<DataTypeInt8>(arguments, type_res)
            ||    checkLeftType<DataTypeInt16>(arguments, type_res)
            ||    checkLeftType<DataTypeInt32>(arguments, type_res)
            ||    checkLeftType<DataTypeInt64>(arguments, type_res)
            ||    checkLeftType<DataTypeFloat32>(arguments, type_res)
            ||    checkLeftType<DataTypeFloat64>(arguments, type_res)))
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type_res;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!(  executeLeftType<DataTypeDate>(block, arguments, result)
            ||  executeLeftType<DataTypeDateTime>(block, arguments, result)
            ||  executeLeftType<DataTypeUInt8>(block, arguments, result)
            ||    executeLeftType<DataTypeUInt16>(block, arguments, result)
            ||    executeLeftType<DataTypeUInt32>(block, arguments, result)
            ||    executeLeftType<DataTypeUInt64>(block, arguments, result)
            ||    executeLeftType<DataTypeInt8>(block, arguments, result)
            ||    executeLeftType<DataTypeInt16>(block, arguments, result)
            ||    executeLeftType<DataTypeInt32>(block, arguments, result)
            ||    executeLeftType<DataTypeInt64>(block, arguments, result)
            ||    executeLeftType<DataTypeFloat32>(block, arguments, result)
            ||    executeLeftType<DataTypeFloat64>(block, arguments, result)))
           throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
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
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionUnaryArithmetic>(); }

private:
    template <typename T0>
    bool checkType(const DataTypes & arguments, DataTypePtr & result) const
    {
        if (typeid_cast<const T0 *>(&*arguments[0]))
        {
            result = std::make_shared<DataTypeNumber<typename Op<typename T0::FieldType>::ResultType>>();
            return true;
        }
        return false;
    }

    template <typename T0>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnVector<T0> * col = typeid_cast<const ColumnVector<T0> *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            using ResultType = typename Op<T0>::ResultType;

            std::shared_ptr<ColumnVector<ResultType>> col_res = std::make_shared<ColumnVector<ResultType>>();
            block.safeGetByPosition(result).column = col_res;

            typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
            vec_res.resize(col->getData().size());
            UnaryOperationImpl<T0, Op<T0> >::vector(col->getData(), vec_res);

            return true;
        }
        else if (const ColumnConst<T0> * col = typeid_cast<const ColumnConst<T0> *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            using ResultType = typename Op<T0>::ResultType;

            ResultType res = 0;
            UnaryOperationImpl<T0, Op<T0> >::constant(col->getData(), res);

            std::shared_ptr<ColumnConst<ResultType>> col_res = std::make_shared<ColumnConst<ResultType>>(col->size(), res);
            block.safeGetByPosition(result).column = col_res;

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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr result;

        if (!(    checkType<DataTypeUInt8>(arguments, result)
            ||    checkType<DataTypeUInt16>(arguments, result)
            ||    checkType<DataTypeUInt32>(arguments, result)
            ||    checkType<DataTypeUInt64>(arguments, result)
            ||    checkType<DataTypeInt8>(arguments, result)
            ||    checkType<DataTypeInt16>(arguments, result)
            ||    checkType<DataTypeInt32>(arguments, result)
            ||    checkType<DataTypeInt64>(arguments, result)
            ||    checkType<DataTypeFloat32>(arguments, result)
            ||    checkType<DataTypeFloat64>(arguments, result)))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return result;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!(    executeType<UInt8>(block, arguments, result)
            ||    executeType<UInt16>(block, arguments, result)
            ||    executeType<UInt32>(block, arguments, result)
            ||    executeType<UInt64>(block, arguments, result)
            ||    executeType<Int8>(block, arguments, result)
            ||    executeType<Int16>(block, arguments, result)
            ||    executeType<Int32>(block, arguments, result)
            ||    executeType<Int64>(block, arguments, result)
            ||    executeType<Float32>(block, arguments, result)
            ||    executeType<Float64>(block, arguments, result)))
           throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return FunctionUnaryArithmeticMonotonicity<Name>::has();
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return FunctionUnaryArithmeticMonotonicity<Name>::get(left, right);
    }
};


struct NamePlus             { static constexpr auto name = "plus"; };
struct NameMinus             { static constexpr auto name = "minus"; };
struct NameMultiply         { static constexpr auto name = "multiply"; };
struct NameDivideFloating    { static constexpr auto name = "divide"; };
struct NameDivideIntegral    { static constexpr auto name = "intDiv"; };
struct NameDivideIntegralOrZero    { static constexpr auto name = "intDivOrZero"; };
struct NameModulo            { static constexpr auto name = "modulo"; };
struct NameNegate            { static constexpr auto name = "negate"; };
struct NameAbs                { static constexpr auto name = "abs"; };
struct NameBitAnd            { static constexpr auto name = "bitAnd"; };
struct NameBitOr            { static constexpr auto name = "bitOr"; };
struct NameBitXor            { static constexpr auto name = "bitXor"; };
struct NameBitNot            { static constexpr auto name = "bitNot"; };
struct NameBitShiftLeft        { static constexpr auto name = "bitShiftLeft"; };
struct NameBitShiftRight    { static constexpr auto name = "bitShiftRight"; };
struct NameBitRotateLeft    { static constexpr auto name = "bitRotateLeft"; };
struct NameBitRotateRight    { static constexpr auto name = "bitRotateRight"; };
struct NameLeast            { static constexpr auto name = "least"; };
struct NameGreatest            { static constexpr auto name = "greatest"; };

using FunctionPlus = FunctionBinaryArithmetic<PlusImpl, NamePlus>;
using FunctionMinus = FunctionBinaryArithmetic<MinusImpl, NameMinus>;
using FunctionMultiply = FunctionBinaryArithmetic<MultiplyImpl, NameMultiply>;
using FunctionDivideFloating = FunctionBinaryArithmetic<DivideFloatingImpl, NameDivideFloating>;
using FunctionDivideIntegral = FunctionBinaryArithmetic<DivideIntegralImpl, NameDivideIntegral>;
using FunctionDivideIntegralOrZero = FunctionBinaryArithmetic<DivideIntegralOrZeroImpl, NameDivideIntegralOrZero>;
using FunctionModulo = FunctionBinaryArithmetic<ModuloImpl, NameModulo>;
using FunctionNegate = FunctionUnaryArithmetic<NegateImpl, NameNegate, true>;
using FunctionAbs = FunctionUnaryArithmetic<AbsImpl, NameAbs, false>;
using FunctionBitAnd = FunctionBinaryArithmetic<BitAndImpl,    NameBitAnd>;
using FunctionBitOr = FunctionBinaryArithmetic<BitOrImpl, NameBitOr>;
using FunctionBitXor = FunctionBinaryArithmetic<BitXorImpl, NameBitXor>;
using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot, true>;
using FunctionBitShiftLeft = FunctionBinaryArithmetic<BitShiftLeftImpl,    NameBitShiftLeft>;
using FunctionBitShiftRight = FunctionBinaryArithmetic<BitShiftRightImpl, NameBitShiftRight>;
using FunctionBitRotateLeft = FunctionBinaryArithmetic<BitRotateLeftImpl,    NameBitRotateLeft>;
using FunctionBitRotateRight = FunctionBinaryArithmetic<BitRotateRightImpl, NameBitRotateRight>;
using FunctionLeast = FunctionBinaryArithmetic<LeastImpl, NameLeast>;
using FunctionGreatest = FunctionBinaryArithmetic<GreatestImpl, NameGreatest>;

/// Monotonicity properties for some functions.

template <> struct FunctionUnaryArithmeticMonotonicity<NameNegate>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
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
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        return {};
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

        if (unlikely(std::is_signed<B>::value && b == -1))
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

        if (unlikely((std::is_signed<B>::value && b == -1) || b == 1))
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
            c[i] = a[i] - (a[i] / divider) * b;    /// NOTE: perhaps, the division semantics with the remainder of negative numbers is not preserved.
    }
};


/** Specializations are specified for dividing numbers of the type UInt64 and UInt32 by the numbers of the same sign.
  * Can be expanded to all possible combinations, but more code is needed.
  */

template <> struct BinaryOperationImpl<UInt64, UInt8,     DivideIntegralImpl<UInt64, UInt8>>     : DivideIntegralByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16,    DivideIntegralImpl<UInt64, UInt16>> : DivideIntegralByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32,     DivideIntegralImpl<UInt64, UInt32>> : DivideIntegralByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64,     DivideIntegralImpl<UInt64, UInt64>> : DivideIntegralByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8,     DivideIntegralImpl<UInt32, UInt8>>     : DivideIntegralByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16,     DivideIntegralImpl<UInt32, UInt16>> : DivideIntegralByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32,     DivideIntegralImpl<UInt32, UInt32>> : DivideIntegralByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64,     DivideIntegralImpl<UInt32, UInt64>> : DivideIntegralByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8,     DivideIntegralImpl<Int64, Int8>>     : DivideIntegralByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16,     DivideIntegralImpl<Int64, Int16>>     : DivideIntegralByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32,     DivideIntegralImpl<Int64, Int32>>     : DivideIntegralByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64,     DivideIntegralImpl<Int64, Int64>>     : DivideIntegralByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8,     DivideIntegralImpl<Int32, Int8>>     : DivideIntegralByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16,     DivideIntegralImpl<Int32, Int16>>     : DivideIntegralByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32,     DivideIntegralImpl<Int32, Int32>>     : DivideIntegralByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64,     DivideIntegralImpl<Int32, Int64>>     : DivideIntegralByConstantImpl<Int32, Int64> {};


template <> struct BinaryOperationImpl<UInt64, UInt8,     ModuloImpl<UInt64, UInt8>>     : ModuloByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16,    ModuloImpl<UInt64, UInt16>> : ModuloByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32,     ModuloImpl<UInt64, UInt32>> : ModuloByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64,     ModuloImpl<UInt64, UInt64>> : ModuloByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8,     ModuloImpl<UInt32, UInt8>>     : ModuloByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16,     ModuloImpl<UInt32, UInt16>> : ModuloByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32,     ModuloImpl<UInt32, UInt32>> : ModuloByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64,     ModuloImpl<UInt32, UInt64>> : ModuloByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8,     ModuloImpl<Int64, Int8>>     : ModuloByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16,     ModuloImpl<Int64, Int16>>     : ModuloByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32,     ModuloImpl<Int64, Int32>>     : ModuloByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64,     ModuloImpl<Int64, Int64>>     : ModuloByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8,     ModuloImpl<Int32, Int8>>     : ModuloByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16,     ModuloImpl<Int32, Int16>>     : ModuloByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32,     ModuloImpl<Int32, Int32>>     : ModuloByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64,     ModuloImpl<Int32, Int64>>     : ModuloByConstantImpl<Int32, Int64> {};

}
