#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct SqrtName { static constexpr auto name = "sqrt"; };
using FunctionSqrt = FunctionMathUnary<UnaryFunctionVectorized<SqrtName, sqrt>>;

}

REGISTER_FUNCTION(Sqrt)
{
    FunctionDocumentation::Description description = R"(
Returns the square root of the argument.
)";
    FunctionDocumentation::Syntax syntax = "sqrt(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The number for which to find the square root of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the square root of x", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT sqrt(16);", "4"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSqrt>(documentation, FunctionFactory::Case::Insensitive);
}

}
