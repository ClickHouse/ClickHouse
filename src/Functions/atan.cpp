#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

struct AtanName { static constexpr auto name = "atan"; };
using FunctionAtan = FunctionMathUnary<UnaryFunctionVectorized<AtanName, atan>>;

}

REGISTER_FUNCTION(Atan)
{
    FunctionDocumentation::Description description = R"(
Returns the arc tangent of the argument.
)";
    FunctionDocumentation::Syntax syntax = "atan(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The value for which to find arc tangent of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the arc tangent of `x`.", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT atan(1);", "0.7853981633974483"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAtan>(documentation, FunctionFactory::Case::Insensitive);
}

}
