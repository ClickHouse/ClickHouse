#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/Native.h>
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
#include <boost/integer/common_factor.hpp>

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
    extern const int ILLEGAL_DIVISION;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
    extern const int DECIMAL_OVERFLOW;
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

    static ResultType constant_constant(A a, B b)
    {
        return Op::template apply<ResultType>(a, b);
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
    using ArrayA = typename ColumnVector<A>::Container;
    using ArrayC = typename ColumnVector<ResultType>::Container;

    static void NO_INLINE vector(const ArrayA & a, ArrayC & c)
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
    static const constexpr bool allow_decimal = true;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        /// Next everywhere, static_cast - so that there is no wrong result in expressions of the form Int64 c = UInt32(a) * Int32(-1).
        return static_cast<Result>(a) + b;
    }

    template <typename Result = ResultType>
    static inline bool apply(A a, B b, Result & c)
    {
        return common::addOverflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateAdd(left, right) : b.CreateFAdd(left, right);
    }
#endif
};


template <typename A, typename B>
struct MultiplyImpl
{
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_decimal = true;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) * b;
    }

    template <typename Result = ResultType>
    static inline bool apply(A a, B b, Result & c)
    {
        return common::mulOverflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateMul(left, right) : b.CreateFMul(left, right);
    }
#endif
};

template <typename A, typename B>
struct MinusImpl
{
    using ResultType = typename NumberTraits::ResultOfSubtraction<A, B>::Type;
    static const constexpr bool allow_decimal = true;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) - b;
    }

    template <typename Result = ResultType>
    static inline bool apply(A a, B b, Result & c)
    {
        return common::subOverflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateSub(left, right) : b.CreateFSub(left, right);
    }
#endif
};

template <typename A, typename B>
struct DivideFloatingImpl
{
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;
    static const constexpr bool allow_decimal = true;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) / b;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (left->getType()->isIntegerTy())
            throw Exception("DivideFloatingImpl expected a floating-point type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateFDiv(left, right);
    }
#endif
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

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
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

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO implement the checks
#endif
};

template <typename A, typename B>
struct ModuloImpl
{
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(typename NumberTraits::ToInteger<A>::Type(a), typename NumberTraits::ToInteger<B>::Type(b));
        return typename NumberTraits::ToInteger<A>::Type(a) % typename NumberTraits::ToInteger<B>::Type(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
};

template <typename A, typename B>
struct BitAndImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) & static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitAndImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateAnd(left, right);
    }
#endif
};

template <typename A, typename B>
struct BitOrImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) | static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitOrImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateOr(left, right);
    }
#endif
};

template <typename A, typename B>
struct BitXorImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) ^ static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitXorImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateXor(left, right);
    }
#endif
};

template <typename A, typename B>
struct BitShiftLeftImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) << static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitShiftLeftImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateShl(left, right);
    }
#endif
};

template <typename A, typename B>
struct BitShiftRightImpl
{
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return static_cast<Result>(a) >> static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitShiftRightImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return is_signed ? b.CreateAShr(left, right) : b.CreateLShr(left, right);
    }
#endif
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

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitRotateLeftImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        auto * size = llvm::ConstantInt::get(left->getType(), left->getType()->getPrimitiveSizeInBits());
        /// XXX how is this supposed to behave in signed mode?
        return b.CreateOr(b.CreateShl(left, right), b.CreateLShr(left, b.CreateSub(size, right)));
    }
#endif
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

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitRotateRightImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        auto * size = llvm::ConstantInt::get(left->getType(), left->getType()->getPrimitiveSizeInBits());
        return b.CreateOr(b.CreateLShr(left, right), b.CreateShl(left, b.CreateSub(size, right)));
    }
#endif
};

template <typename A, typename B>
struct BitTestImpl
{
    using ResultType = UInt8;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        return (typename NumberTraits::ToInteger<A>::Type(a) >> typename NumberTraits::ToInteger<B>::Type(b)) & 1;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO
#endif
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

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        if (!left->getType()->isIntegerTy())
            /// XXX minnum is basically fmin(), it may or may not match whatever apply() does
            return b.CreateMinNum(left, right);
        return b.CreateSelect(is_signed ? b.CreateICmpSLT(left, right) : b.CreateICmpULT(left, right), left, right);
    }
#endif
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

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// ???
#endif
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

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool is_signed)
    {
        if (!left->getType()->isIntegerTy())
            /// XXX maxnum is basically fmax(), it may or may not match whatever apply() does
            /// XXX CreateMaxNum is broken on LLVM 5.0 and 6.0 (generates minnum instead; fixed in 7)
            return b.CreateBinaryIntrinsic(llvm::Intrinsic::maxnum, left, right);
        return b.CreateSelect(is_signed ? b.CreateICmpSGT(left, right) : b.CreateICmpUGT(left, right), left, right);
    }
#endif
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

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// ???
#endif
};

template <typename A, typename B>
using GreatestImpl = std::conditional_t<!NumberTraits::LeastGreatestSpecialCase<A, B>, GreatestBaseImpl<A, B>, GreatestSpecialImpl<A, B>>;


template <typename A>
struct NegateImpl
{
    using ResultType = std::conditional_t<decTrait<A>(), A, typename NumberTraits::ResultOfNegate<A>::Type>;

    static inline ResultType apply(A a)
    {
        return -static_cast<ResultType>(a);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        return arg->getType()->isIntegerTy() ? b.CreateNeg(arg) : b.CreateFNeg(arg);
    }
#endif
};

template <typename A>
struct BitNotImpl
{
    using ResultType = typename NumberTraits::ResultOfBitNot<A>::Type;

    static inline ResultType apply(A a)
    {
        return ~static_cast<ResultType>(a);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        if (!arg->getType()->isIntegerTy())
            throw Exception("BitNotImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateNot(arg);
    }
#endif
};

template <typename A>
struct AbsImpl
{
    using ResultType = std::conditional_t<decTrait<A>(), A, typename NumberTraits::ResultOfAbs<A>::Type>;

    static inline ResultType apply(A a)
    {
        if constexpr (decTrait<A>())
            return a < 0 ? A(-a) : a;
        else if constexpr (std::is_integral_v<A> && std::is_signed_v<A>)
            return a < 0 ? static_cast<ResultType>(~a) + 1 : a;
        else if constexpr (std::is_integral_v<A> && std::is_unsigned_v<A>)
            return static_cast<ResultType>(a);
        else if constexpr (std::is_floating_point_v<A>)
            return static_cast<ResultType>(std::abs(a));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// special type handling, some other time
#endif
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
        return boost::integer::gcd(
            typename NumberTraits::ToInteger<Result>::Type(a),
            typename NumberTraits::ToInteger<Result>::Type(b));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// exceptions (and a non-trivial algorithm)
#endif
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
        return boost::integer::lcm(
            typename NumberTraits::ToInteger<Result>::Type(a),
            typename NumberTraits::ToInteger<Result>::Type(b));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// exceptions (and a non-trivial algorithm)
#endif
};

template <typename A>
struct IntExp2Impl
{
    using ResultType = UInt64;

    static inline ResultType apply(A a)
    {
        return intExp2(a);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        if (!arg->getType()->isIntegerTy())
            throw Exception("IntExp2Impl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateShl(llvm::ConstantInt::get(arg->getType(), 1), arg);
    }
#endif
};

template <typename A>
struct IntExp10Impl
{
    using ResultType = UInt64;

    static inline ResultType apply(A a)
    {
        return intExp10(a);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// library function
#endif
};


template <typename T> struct NativeType { using Type = T; };
template <> struct NativeType<Dec32> { using Type = Int32; };
template <> struct NativeType<Dec64> { using Type = Int64; };
template <> struct NativeType<Dec128> { using Type = Int128; };

/// Binary operations for Decimals need scale args
/// +|- scale one of args (which scale factor is not 1). ScaleR = oneof(Scale1, Scale2);
/// *   no agrs scale. ScaleR = Scale1 + Scale2;
/// /   first arg scale. ScaleR = Scale1 (scale_a = DecimalType<B>::getScale()).
template <typename A, typename B, template <typename, typename> typename Operation, typename ResultType_>
struct DecimalBinaryOperation
{
    using ResultType = ResultType_;
    using NativeResultType = typename NativeType<ResultType>::Type;
    using Op = Operation<NativeResultType, NativeResultType>;
    using ArrayA = typename ColumnVector<A>::Container;
    using ArrayB = typename ColumnVector<B>::Container;
    using ArrayC = typename ColumnVector<ResultType>::Container;

    static constexpr bool is_plus_minus =   std::is_same_v<Operation<Int32, Int32>, PlusImpl<Int32, Int32>> ||
                                            std::is_same_v<Operation<Int32, Int32>, MinusImpl<Int32, Int32>>;
    static constexpr bool is_multiply =     std::is_same_v<Operation<Int32, Int32>, MultiplyImpl<Int32, Int32>>;
    static constexpr bool is_division =     std::is_same_v<Operation<Int32, Int32>, DivideFloatingImpl<Int32, Int32>>;
    static constexpr bool is_compare =      std::is_same_v<Operation<Int32, Int32>, LeastBaseImpl<Int32, Int32>> ||
                                            std::is_same_v<Operation<Int32, Int32>, GreatestBaseImpl<Int32, Int32>>;
    static constexpr bool is_plus_minus_compare = is_plus_minus || is_compare;
    static constexpr bool can_overflow = is_plus_minus || is_multiply;

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
        else if constexpr (is_division && decTrait<B>())
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
        else if constexpr (is_division && decTrait<B>())
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
        else if constexpr (is_division && decTrait<B>())
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
        else if constexpr (is_division && decTrait<B>())
            return applyScaledDiv(a, b, scale_a);
        return apply(a, b);
    }

private:
    /// there's implicit type convertion here
    static NativeResultType apply(NativeResultType a, NativeResultType b)
    {
        if constexpr (can_overflow)
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
    static NativeResultType applyScaled(NativeResultType a, NativeResultType b, NativeResultType scale)
    {
        if constexpr (is_plus_minus_compare)
        {
            NativeResultType res;

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

            return res;
        }
    }

    static NativeResultType applyScaledDiv(NativeResultType a, NativeResultType b, NativeResultType scale)
    {
        if constexpr (is_division)
        {
            bool overflow = false;
            if constexpr (!decTrait<A>())
                overflow |= common::mulOverflow(scale, scale, scale);
            overflow |= common::mulOverflow(a, scale, a);
            if (overflow)
                throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);

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

template <typename DataType> constexpr bool IsDateOrDateTime = false;
template <> constexpr bool IsDateOrDateTime<DataTypeDate> = true;
template <> constexpr bool IsDateOrDateTime<DataTypeDateTime> = true;

template <typename DataType> constexpr bool IsDecimal = false;
template <> constexpr bool IsDecimal<DataTypeDecimal<Dec32>> = true;
template <> constexpr bool IsDecimal<DataTypeDecimal<Dec64>> = true;
template <> constexpr bool IsDecimal<DataTypeDecimal<Dec128>> = true;

template <typename T0, typename T1> constexpr bool UseLeftDecimal = false;
template <> constexpr bool UseLeftDecimal<DataTypeDecimal<Dec128>, DataTypeDecimal<Dec32>> = true;
template <> constexpr bool UseLeftDecimal<DataTypeDecimal<Dec128>, DataTypeDecimal<Dec64>> = true;
template <> constexpr bool UseLeftDecimal<DataTypeDecimal<Dec64>, DataTypeDecimal<Dec32>> = true;

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
        std::is_same_v<Operation<T0, T0>, LeastBaseImpl<T0, T0>> ||
        std::is_same_v<Operation<T0, T0>, GreatestBaseImpl<T0, T0>>;

    /// Appropriate result type for binary operator on numeric types. "Date" can also mean
    /// DateTime, but if both operands are Dates, their type must be the same (e.g. Date - DateTime is invalid).
    using ResultDataType = Switch<
        /// Decimal cases
        Case<!allow_decimal && (IsDecimal<LeftDataType> || IsDecimal<RightDataType>), InvalidType>,
        Case<IsDecimal<LeftDataType> && IsDecimal<RightDataType> && UseLeftDecimal<LeftDataType, RightDataType>, LeftDataType>,
        Case<IsDecimal<LeftDataType> && IsDecimal<RightDataType>, RightDataType>,
        Case<IsDecimal<LeftDataType> && !IsDecimal<RightDataType> && IsIntegral<RightDataType>, LeftDataType>,
        Case<!IsDecimal<LeftDataType> && IsDecimal<RightDataType> && IsIntegral<LeftDataType>, RightDataType>,
        /// Decimal <op> Real is not supported (traditional DBs convert Decimal <op> Real to Real)
        Case<IsDecimal<LeftDataType> && !IsDecimal<RightDataType> && !IsIntegral<RightDataType>, InvalidType>,
        Case<!IsDecimal<LeftDataType> && IsDecimal<RightDataType> && !IsIntegral<LeftDataType>, InvalidType>,
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
        Case<std::is_same_v<LeftDataType, RightDataType> && (std::is_same_v<Op, LeastImpl<T0, T1>> || std::is_same_v<Op, GreatestImpl<T0, T1>>),
            LeftDataType>>;
};


template <typename... Ts, typename F>
static bool castTypeToEither(const IDataType * type, F && f)
{
    /// XXX can't use && here because gcc-7 complains about parentheses around && within ||
    return ((typeid_cast<const Ts *>(type) ? f(*typeid_cast<const Ts *>(type)) : false) || ...);
}


template <template <typename, typename> class Op, typename Name, bool CanBeExecutedOnDefaultArguments = true>
class FunctionBinaryArithmetic : public IFunction
{
    const Context & context;

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
            DataTypeDecimal<Dec32>,
            DataTypeDecimal<Dec64>,
            DataTypeDecimal<Dec128>
        >(type, std::forward<F>(f));
    }

    template <typename F>
    static bool castBothTypes(const IDataType * left, const IDataType * right, F && f)
    {
        return castType(left, [&](const auto & left) { return castType(right, [&](const auto & right) { return f(left, right); }); });
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
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBinaryArithmetic>(context); }

    FunctionBinaryArithmetic(const Context & context) : context(context) {}

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
        bool valid = castBothTypes(arguments[0].get(), arguments[1].get(), [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;
            if constexpr (!std::is_same_v<ResultDataType, InvalidType>)
            {
                if constexpr (IsDecimal<LeftDataType> && IsDecimal<RightDataType>)
                {
                    constexpr bool is_multiply = std::is_same_v<Op<UInt8, UInt8>, MultiplyImpl<UInt8, UInt8>>;
                    constexpr bool is_division = std::is_same_v<Op<UInt8, UInt8>, DivideFloatingImpl<UInt8, UInt8>>;
                    ResultDataType result_type = decimalResultType(left, right, is_multiply, is_division);
                    type_res = std::make_shared<ResultDataType>(result_type.getPrecision(), result_type.getScale());
                }
                else if constexpr (IsDecimal<LeftDataType>)
                    type_res = std::make_shared<LeftDataType>(left.getPrecision(), left.getScale());
                else if constexpr (IsDecimal<RightDataType>)
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

            function->execute(new_block, new_arguments, result, input_rows_count);
            block.getByPosition(result).column = new_block.getByPosition(result).column;

            return;
        }

        auto * left = block.getByPosition(arguments[0]).type.get();
        auto * right = block.getByPosition(arguments[1]).type.get();
        bool valid = castBothTypes(left, right, [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;
            if constexpr (!std::is_same_v<ResultDataType, InvalidType>)
            {
                constexpr bool result_is_decimal = IsDecimal<LeftDataType> || IsDecimal<RightDataType>;
                constexpr bool is_multiply = std::is_same_v<Op<UInt8, UInt8>, MultiplyImpl<UInt8, UInt8>>;
                constexpr bool is_division = std::is_same_v<Op<UInt8, UInt8>, DivideFloatingImpl<UInt8, UInt8>>;

                using T0 = typename LeftDataType::FieldType;
                using T1 = typename RightDataType::FieldType;
                using ResultType = typename ResultDataType::FieldType;
                using ColVecT0 = ColumnVector<T0>;
                using ColVecT1 = ColumnVector<T1>;
                using ColVecResult = ColumnVector<ResultType>;

                /// Decimal operations need scale. Operations are on result type.
                using OpImpl = std::conditional_t<IsDecimal<ResultDataType>,
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
                            if constexpr (IsDecimal<RightDataType> && is_division)
                                scale_a = right.getScaleMultiplier();
                            auto res = OpImpl::constant_constant(col_left->template getValue<T0>(), col_right->template getValue<T1>(),
                                                                 scale_a, scale_b);
                            block.getByPosition(result).column =
                                ResultDataType(type.getPrecision(), type.getScale()).createColumnConst(col_left->size(), toField(res));
                        }
                        else
                        {
                            auto res = OpImpl::constant_constant(col_left->template getValue<T0>(), col_right->template getValue<T1>());
                            block.getByPosition(result).column = ResultDataType().createColumnConst(col_left->size(), toField(res));
                        }
                        return true;
                    }
                }

                auto col_res = ColVecResult::create();
                auto & vec_res = col_res->getData();
                vec_res.resize(block.rows());
                if (auto col_left = checkAndGetColumnConst<ColVecT0>(col_left_raw))
                {
                    if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                    {
                        if constexpr (result_is_decimal)
                        {
                            ResultDataType type = decimalResultType(left, right, is_multiply, is_division);
                            vec_res.setScale(type.getScale());

                            typename ResultDataType::FieldType scale_a = type.scaleFactorFor(left, is_multiply);
                            typename ResultDataType::FieldType scale_b = type.scaleFactorFor(right, is_multiply || is_division);
                            if constexpr (IsDecimal<RightDataType> && is_division)
                                scale_a = right.getScaleMultiplier();
                            OpImpl::constant_vector(col_left->template getValue<T0>(), col_right->getData(), vec_res, scale_a, scale_b);
                        }
                        else
                            OpImpl::constant_vector(col_left->template getValue<T0>(), col_right->getData(), vec_res);
                    }
                    else
                        return false;
                }
                else if (auto col_left = checkAndGetColumn<ColVecT0>(col_left_raw))
                {
                    if constexpr (result_is_decimal)
                    {
                        ResultDataType type = decimalResultType(left, right, is_multiply, is_division);
                        vec_res.setScale(type.getScale());

                        typename ResultDataType::FieldType scale_a = type.scaleFactorFor(left, is_multiply);
                        typename ResultDataType::FieldType scale_b = type.scaleFactorFor(right, is_multiply || is_division);
                        if constexpr (IsDecimal<RightDataType> && is_division)
                            scale_a = right.getScaleMultiplier();
                        if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                            OpImpl::vector_vector(col_left->getData(), col_right->getData(), vec_res, scale_a, scale_b);
                        else if (auto col_right = checkAndGetColumnConst<ColVecT1>(col_right_raw))
                            OpImpl::vector_constant(col_left->getData(), col_right->template getValue<T1>(), vec_res, scale_a, scale_b);
                        else
                            return false;
                    }
                    else
                    {
                        if (auto col_right = checkAndGetColumn<ColVecT1>(col_right_raw))
                            OpImpl::vector_vector(col_left->getData(), col_right->getData(), vec_res);
                        else if (auto col_right = checkAndGetColumnConst<ColVecT1>(col_right_raw))
                            OpImpl::vector_constant(col_left->getData(), col_right->template getValue<T1>(), vec_res);
                        else
                            return false;
                    }
                }
                else
                {
                    return false;
                }
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
            return !std::is_same_v<ResultDataType, InvalidType> && !IsDecimal<ResultDataType> && OpSpec::compilable;
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
            if constexpr (!std::is_same_v<ResultDataType, InvalidType> && !IsDecimal<ResultDataType> && OpSpec::compilable)
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


template <typename FunctionName>
struct FunctionUnaryArithmeticMonotonicity;


template <template <typename> class Op, typename Name, bool is_injective>
class FunctionUnaryArithmetic : public IFunction
{
    static constexpr bool allow_decimal = std::is_same_v<Op<Int8>, NegateImpl<Int8>> || std::is_same_v<Op<Int8>, AbsImpl<Int8>>;

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
            DataTypeDecimal<Dec32>,
            DataTypeDecimal<Dec64>,
            DataTypeDecimal<Dec128>
        >(type, std::forward<F>(f));
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUnaryArithmetic>(); }

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
        bool valid = castType(arguments[0].get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;
            using T0 = typename DataType::FieldType;

            if constexpr (IsDecimal<DataType>)
            {
                if constexpr (!allow_decimal)
                    return false;
                result = std::make_shared<DataType>(type.getPrecision(), type.getScale());
            }
            else
                result = std::make_shared<DataTypeNumber<typename Op<T0>::ResultType>>();
            return true;
        });
        if (!valid)
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return result;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        bool valid = castType(block.getByPosition(arguments[0]).type.get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;
            using T0 = typename DataType::FieldType;

            if constexpr (IsDecimal<DataType>)
            {
                if constexpr (allow_decimal)
                {
                    if (auto col = checkAndGetColumn<ColumnVector<T0>>(block.getByPosition(arguments[0]).column.get()))
                    {
                        auto col_res = ColumnVector<typename Op<T0>::ResultType>::create();
                        auto & vec_res = col_res->getData();
                        vec_res.resize(col->getData().size());
                        UnaryOperationImpl<T0, Op<T0>>::vector(col->getData(), vec_res);
                        block.getByPosition(result).column = std::move(col_res);
                        return true;
                    }
                }
            }
            else
            {
                if (auto col = checkAndGetColumn<ColumnVector<T0>>(block.getByPosition(arguments[0]).column.get()))
                {
                    auto col_res = ColumnVector<typename Op<T0>::ResultType>::create();
                    auto & vec_res = col_res->getData();
                    vec_res.resize(col->getData().size());
                    UnaryOperationImpl<T0, Op<T0>>::vector(col->getData(), vec_res);
                    block.getByPosition(result).column = std::move(col_res);
                    return true;
                }
            }

            return false;
        });
        if (!valid)
            throw Exception(getName() + "'s argument does not match the expected data type", ErrorCodes::LOGICAL_ERROR);
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & arguments) const override
    {
        return castType(arguments[0].get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;
            return !IsDecimal<DataType> && Op<typename DataType::FieldType>::compilable;
        });
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, ValuePlaceholders values) const override
    {
        llvm::Value * result = nullptr;
        castType(types[0].get(), [&](const auto & type)
        {
            using DataType = std::decay_t<decltype(type)>;
            using T0 = typename DataType::FieldType;
            using T1 = typename Op<T0>::ResultType;
            if constexpr (!std::is_same_v<T1, InvalidType> && !IsDecimal<DataType> && Op<T0>::compilable)
            {
                auto & b = static_cast<llvm::IRBuilder<> &>(builder);
                auto * v = nativeCast(b, types[0], values[0](), std::make_shared<DataTypeNumber<T1>>());
                result = Op<T0>::compile(b, v, std::is_signed_v<T1>);
                return true;
            }
            return false;
        });
        return result;
    }
#endif

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
using FunctionDivideIntegral = FunctionBinaryArithmetic<DivideIntegralImpl, NameDivideIntegral, false>;
using FunctionDivideIntegralOrZero = FunctionBinaryArithmetic<DivideIntegralOrZeroImpl, NameDivideIntegralOrZero>;
using FunctionModulo = FunctionBinaryArithmetic<ModuloImpl, NameModulo, false>;
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
using FunctionGCD = FunctionBinaryArithmetic<GCDImpl, NameGCD, false>;
using FunctionLCM = FunctionBinaryArithmetic<LCMImpl, NameLCM, false>;
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
            throw Exception{"Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 2.", ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION};

        const auto first_arg = arguments.front().get();

        if (!first_arg->isInteger())
            throw Exception{"Illegal type " + first_arg->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};


        for (const auto i : ext::range(1, arguments.size()))
        {
            const auto pos_arg = arguments[i].get();

            if (!pos_arg->isUnsignedInteger())
                throw Exception{"Illegal type " + pos_arg->getName() + " of " + toString(i) + " argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block , const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
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
            throw Exception{"Illegal column " + value_col->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
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
                throw Exception{"Illegal column " + pos_col->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
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
    static inline UInt8 apply(A a, B b) { return (a & b) != 0; }
};

struct BitTestAllImpl
{
    template <typename A, typename B>
    static inline UInt8 apply(A a, B b) { return (a & b) == b; }
};


using FunctionBitTestAny = FunctionBitTestMany<BitTestAnyImpl, NameBitTestAny>;
using FunctionBitTestAll = FunctionBitTestMany<BitTestAllImpl, NameBitTestAll>;

}
