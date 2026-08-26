#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB
{
namespace
{

struct AcoshName
{
    static constexpr auto name = "acosh";
};
using FunctionAcosh = FunctionMathUnary<UnaryFunctionVectorized<AcoshName, acosh>>;

}

REGISTER_FUNCTION(Acosh)
{
    FunctionDocumentation::Description description = R"(
Returns the inverse hyperbolic cosine.
)";
    FunctionDocumentation::Syntax syntax = "acosh(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Hyperbolic cosine of angle. Values from the interval: `1 ≤ x < +∞`.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the angle, in radians. Values from the interval: `0 ≤ acosh(x) < +∞`.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT acosh(1)", "0"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAcosh>(documentation);
}

}
