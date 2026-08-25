#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct ErfName { static constexpr auto name = "erf"; };
using FunctionErf = FunctionMathUnary<UnaryFunctionVectorized<ErfName, std::erf>>;

}

REGISTER_FUNCTION(Erf)
{
    FunctionDocumentation::Description description = R"(
If `x` is non-negative, then `erf(x/(σ√2))` is the probability that a random variable having a normal distribution with standard deviation `σ` takes the value that is separated from the expected value by more than `x`.
)";
    FunctionDocumentation::Syntax syntax = "erf(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The value for which to compute the error function value.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the error function value", {"Float*"}};
    FunctionDocumentation::Examples examples = {
        {"Three sigma rule", "SELECT erf(3 / sqrt(2))", R"(
┌─erf(divide(3, sqrt(2)))─┐
│      0.9973002039367398 │
└─────────────────────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionErf>(documentation);
}

}
