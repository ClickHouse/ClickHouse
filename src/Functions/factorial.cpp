#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/NumberTraits.h>
#include <Common/FieldVisitorConvertToNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

template <typename A>
struct FactorialImpl
{
    using ResultType = UInt64;
    static const constexpr bool allow_decimal = false;
    static const constexpr bool allow_string_or_fixed_string = false;

    static NO_SANITIZE_UNDEFINED ResultType apply(A a)
    {
        if constexpr (is_floating_point<A> || is_over_big_int<A>)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type of argument of function factorial, should not be floating point or big int");

        if constexpr (is_integer<A>)
        {
            if (a > 20)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The maximum value for the input argument of function factorial is 20");

            if constexpr (is_unsigned_v<A>)
                return factorials[a];
            else if constexpr (is_signed_v<A>)
                return a >= 0 ? factorials[a] : 1;
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// special type handling, some other time
#endif

private:
    static const constexpr ResultType factorials[21]
        = {1,
           1,
           2,
           6,
           24,
           120,
           720,
           5040,
           40320,
           362880,
           3628800,
           39916800,
           479001600,
           6227020800L,
           87178291200L,
           1307674368000L,
           20922789888000L,
           355687428096000L,
           6402373705728000L,
           121645100408832000L,
           2432902008176640000L};
};

struct NameFactorial { static constexpr auto name = "factorial"; };
using FunctionFactorial = FunctionUnaryArithmetic<FactorialImpl, NameFactorial, false>;

template <> struct FunctionUnaryArithmeticMonotonicity<NameFactorial>
{
    static bool has() { return true; }

    static IFunction::Monotonicity get(const IDataType &, const Field & left, const Field & right)
    {
        bool is_strict = false;
        if (!left.isNull() && !right.isNull())
        {
            auto left_value = applyVisitor(FieldVisitorConvertToNumber<Int128>(), left);
            auto right_value = applyVisitor(FieldVisitorConvertToNumber<Int128>(), left);
            if (1 <= left_value && left_value <= right_value && right_value <= 20)
                is_strict = true;
        }

        return {
            .is_monotonic = true,
            .is_positive = true,
            .is_always_monotonic = true,
            .is_strict = is_strict,
        };
    }
};


REGISTER_FUNCTION(Factorial)
{
    FunctionDocumentation::Description description = R"(
Computes the factorial of an integer value.
The factorial of 0 is 1. Likewise, the `factorial()` function returns `1` for any negative value.
The maximum positive value for the input argument is `20`, a value of `21` or greater will cause an exception.
    )";
    FunctionDocumentation::Syntax syntax = "factorial(n)";
    FunctionDocumentation::Arguments arguments = {
        {"n", "Integer value for which to calculate the factorial. Maximum value is 20.", {"(U)Int8/16/32/64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the factorial of the input as UInt64. Returns 1 for input 0 or any negative value.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "factorial(10)", "3628800"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFactorial>(documentation, FunctionFactory::Case::Insensitive);
}

}
