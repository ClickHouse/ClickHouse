#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB
{
namespace
{
    struct SinhName
    {
        static constexpr auto name = "sinh";
    };
    using FunctionSinh = FunctionMathUnary<UnaryFunctionVectorized<SinhName, sinh>>;

}

REGISTER_FUNCTION(Sinh)
{
    FunctionDocumentation::Description description = R"(
Returns the hyperbolic sine.
)";
    FunctionDocumentation::Syntax syntax = "sinh(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The angle, in radians. Values from the interval: -∞ < x < +∞.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns values from the interval: -∞ < sinh(x) < +∞", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT sinh(0)", "0"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSinh>(documentation);
}

}
