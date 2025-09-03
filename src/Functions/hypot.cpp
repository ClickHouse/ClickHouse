#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathBinaryFloat64.h>

namespace DB
{
namespace
{
    struct HypotName
    {
        static constexpr auto name = "hypot";
    };
    using FunctionHypot = FunctionMathBinaryFloat64<BinaryFunctionVectorized<HypotName, hypot>>;

}

REGISTER_FUNCTION(Hypot)
{
    FunctionDocumentation::Description description = R"(
Returns the length of the hypotenuse of a right-angle triangle.
Hypot avoids problems that occur when squaring very large or very small numbers.
)";
    FunctionDocumentation::Syntax syntax = "hypot(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The first cathetus of a right-angle triangle.", {"(U)Int*", "Float*", "Decimal*"}},
        {"y", "The second cathetus of a right-angle triangle.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the length of the hypotenuse of a right-angle triangle.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT hypot(1, 1)", "1.4142135623730951"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHypot>(documentation, FunctionFactory::Case::Insensitive);
}

}
