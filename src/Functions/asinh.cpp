#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB
{
namespace
{

struct AsinhName
{
    static constexpr auto name = "asinh";
};
using FunctionAsinh = FunctionMathUnary<UnaryFunctionVectorized<AsinhName, asinh>>;

}

REGISTER_FUNCTION(Asinh)
{
    FunctionDocumentation::Description description = R"(
Returns the inverse hyperbolic sine.
)";
    FunctionDocumentation::Syntax syntax = "asinh(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Hyperbolic sine of angle. Values from the interval: `-∞ < x < +∞`.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the angle, in radians. Values from the interval: `-∞ < asinh(x) < +∞`.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT asinh(0)", "0"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAsinh>(documentation);
}

}
