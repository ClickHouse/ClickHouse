#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct TanName { static constexpr auto name = "tan"; };
using FunctionTan = FunctionMathUnary<UnaryFunctionVectorized<TanName, tan>>;

}

REGISTER_FUNCTION(Tan)
{
    FunctionDocumentation::Description description = R"(
Returns the tangent of the argument.
)";
    FunctionDocumentation::Syntax syntax = "tan(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The angle in radians.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the tangent of `x`.", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT tan(0);", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTan>(documentation, FunctionFactory::Case::Insensitive);
}

}
