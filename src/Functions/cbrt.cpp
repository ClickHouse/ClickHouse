#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct CbrtName { static constexpr auto name = "cbrt"; };
using FunctionCbrt = FunctionMathUnary<UnaryFunctionVectorized<CbrtName, cbrt>>;

}

REGISTER_FUNCTION(Cbrt)
{
    FunctionDocumentation::Description description = R"(
Returns the cubic root of the argument.
)";
    FunctionDocumentation::Syntax syntax = "cbrt(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The value for which to find the cubic root of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the cubic root of `x`.", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT cbrt(8);", "2"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCbrt>(documentation);
}

}
