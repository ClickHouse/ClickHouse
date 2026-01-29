#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB
{
namespace
{

struct AtanhName
{
    static constexpr auto name = "atanh";
};
using FunctionAtanh = FunctionMathUnary<UnaryFunctionVectorized<AtanhName, atanh>>;

}

REGISTER_FUNCTION(Atanh)
{
    FunctionDocumentation::Description description = R"(
Returns the inverse hyperbolic tangent.
)";
    FunctionDocumentation::Syntax syntax = "atanh(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Hyperbolic tangent of angle. Values from the interval: -1 < x < 1. `(U)Int*`, `Float*` or `Decimal*`.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the angle, in radians. Values from the interval: -∞ < atanh(x) < +∞", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT atanh(0)", "0"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAtanh>(documentation);
}

}
