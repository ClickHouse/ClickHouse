#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB
{
namespace
{

struct CoshName
{
    static constexpr auto name = "cosh";
};
using FunctionCosh = FunctionMathUnary<UnaryFunctionVectorized<CoshName, cosh>>;

}

REGISTER_FUNCTION(Cosh)
{
    FunctionDocumentation::Description description = R"(
Returns the hyperbolic cosine of the argument.
)";
    FunctionDocumentation::Syntax syntax = "cosh(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The angle, in radians. Values from the interval: `-∞ < x < +∞`.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns values from the interval: `1 ≤ cosh(x) < +∞`", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT cosh(0)", "1"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCosh>(documentation);
}

}
