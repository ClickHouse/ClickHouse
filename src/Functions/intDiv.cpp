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
    FunctionDocumentation::Description description = R"(
Performs an integer division of two values `x` by `y`. In other words it
computes the quotient rounded down to the next smallest integer.

The result has the same width as the dividend (the first parameter).

An exception is thrown when dividing by zero, when the quotient does not fit
in the range of the dividend, or when dividing a minimal negative number by minus one.
    )";
    FunctionDocumentation::Syntax syntax = "intDiv(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "Left hand operand."};
    FunctionDocumentation::Argument argument2 = {"y", "Right hand operand."};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = "Result of integer division of `x` and `y`";
    FunctionDocumentation::Example example1 = {"Integer division of two floats", "SELECT intDiv(toFloat64(1), 0.001) AS res, toTypeName(res)", R"(
┌──res─┬─toTypeName(intDiv(toFloat64(1), 0.001))─┐
│ 1000 │ Int64                                   │
└──────┴─────────────────────────────────────────┘
    )"};
    FunctionDocumentation::Example example2 = {
        "Quotient does not fit in the range of the dividend",
        R"(
SELECT
intDiv(1, 0.001) AS res,
toTypeName(res)
        )",
        R"(
Received exception from server (version 23.2.1):
Code: 153. DB::Exception: Received from localhost:9000. DB::Exception:
Cannot perform integer division, because it will produce infinite or too
large number: While processing intDiv(1, 0.001) AS res, toTypeName(res).
(ILLEGAL_DIVISION)
        )"
    };
    FunctionDocumentation::Examples examples = {example1, example2};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionIntDiv>(documentation);
}

struct NameIntDivOrNull { static constexpr auto name = "intDivOrNull"; };
using FunctionIntDivOrNull = BinaryArithmeticOverloadResolver<DivideIntegralOrNullImpl, NameIntDivOrNull, false>;

REGISTER_FUNCTION(IntDivOrNull)
{
    FunctionDocumentation::Description description = R"(
Same as `intDiv` but returns NULL when dividing by zero or when dividing a
minimal negative number by minus one.
    )";
    FunctionDocumentation::Syntax syntax = "intDivOrNull(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "Left hand operand."};
    FunctionDocumentation::Argument argument2 = {"y", "Right hand operand."};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = "Result of integer division of `x` and `y`, or NULL.";
    FunctionDocumentation::Example example1 = {"Integer division by zero", "SELECT intDivOrNull(1, 0)","\\N"};
    FunctionDocumentation::Example example2 = {"Dividing a minimal negative number by minus 1", "SELECT intDivOrNull(-9223372036854775808, -1)","\\N"};
    FunctionDocumentation::Examples examples = {example1, example2};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionIntDivOrNull>(documentation);
}
}
