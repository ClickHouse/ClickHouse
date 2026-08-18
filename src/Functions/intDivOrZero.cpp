#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>


namespace DB
{

template <typename A, typename B>
struct DivideIntegralOrZeroImpl
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        if (unlikely(divisionLeadsToFPE(a, b)))
            return 0;

        return DivideIntegralImpl<A, B>::template apply<Result>(a, b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO implement the checks
#endif
};

struct NameIntDivOrZero { static constexpr auto name = "intDivOrZero"; };
using FunctionIntDivOrZero = BinaryArithmeticOverloadResolver<DivideIntegralOrZeroImpl, NameIntDivOrZero>;

REGISTER_FUNCTION(IntDivOrZero)
{
    FunctionDocumentation::Description description = R"(
Same as `intDiv` but returns zero when dividing by zero or when dividing a
minimal negative number by minus one.
    )";
    FunctionDocumentation::Syntax syntax = "intDivOrZero(a, b)";
    FunctionDocumentation::Arguments arguments =
    {
        {"a", "Left hand operand.", {"(U)Int*"}},
        {"b", "Right hand operand.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Result of integer division of a and b, or zero."};
    FunctionDocumentation::Examples examples =
    {
        {"Integer division by zero", "SELECT intDivOrZero(1, 0)", "0"},
        {"Dividing a minimal negative number by minus 1", "SELECT intDivOrZero(0.05, -1)", "0"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIntDivOrZero>(documentation);
}

}
