#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct CosName { static constexpr auto name = "cos"; };
using FunctionCos = FunctionMathUnary<UnaryFunctionVectorized<CosName, cos>>;

}

REGISTER_FUNCTION(Cos)
{
    FunctionDocumentation::Description description = R"(
Returns the cosine of the argument.
)";
    FunctionDocumentation::Syntax syntax = "cos(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The angle in radians.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the cosine of `x`.", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT cos(0);", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCos>(documentation, FunctionFactory::Case::Insensitive);
}

}
