#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

#include "divide/divide.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
}

namespace
{

/// Optimizations for integer division by a constant.

template <typename A, typename B>
struct DivideIntegralByConstantImpl
    : BinaryOperation<A, B, DivideIntegralImpl<A, B>>
{
    using Op = DivideIntegralImpl<A, B>;
    using ResultType = typename Op::ResultType;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <OpCase op_case>
    static void NO_INLINE process(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t size, const NullMap * right_nullmap)
    {
        if constexpr (op_case == OpCase::RightConstant)
        {
            if (right_nullmap && (*right_nullmap)[0])
                return;

            vectorConstant(a, *b, c, size);
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

    static void NO_INLINE NO_SANITIZE_UNDEFINED vectorConstant(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size)
    {
        /// Division by -1. By the way, we avoid FPE by division of the largest negative number by -1.
        if (unlikely(is_signed_v<B> && b == -1))
        {
            for (size_t i = 0; i < size; ++i)
                c_pos[i] = -make_unsigned_t<A>(a_pos[i]);   /// Avoid UBSan report in signed integer overflow.
            return;
        }

        /// Division with too large divisor.
        if (unlikely(b > std::numeric_limits<A>::max()
            || (std::is_signed_v<A> && std::is_signed_v<B> && b < std::numeric_limits<A>::lowest())))
        {
            for (size_t i = 0; i < size; ++i)
                c_pos[i] = 0;
            return;
        }

        if (unlikely(static_cast<A>(b) == 0))
            throw Exception(ErrorCodes::ILLEGAL_DIVISION, "Division by zero");

        divideImpl(a_pos, b, c_pos, size);
    }

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

/** Specializations are specified for dividing numbers of the type UInt64, UInt32, Int64, Int32 by the numbers of the same sign.
  * Can be expanded to all possible combinations, but more code is needed.
  */

}

namespace impl_
{
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
}

struct NameIntDiv { static constexpr auto name = "intDiv"; };
using FunctionIntDiv = BinaryArithmeticOverloadResolver<DivideIntegralImpl, NameIntDiv, false>;

REGISTER_FUNCTION(IntDiv)
{
    factory.registerFunction<FunctionIntDiv>();
}

}
