#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct Log2Name { static constexpr auto name = "log2"; };
using FunctionLog2 = FunctionMathUnary<UnaryFunctionVectorized<Log2Name, log2>>;

}

REGISTER_FUNCTION(Log2)
{
    FunctionDocumentation::Description description = R"(
Returns the binary logarithm of the argument.
)";
    FunctionDocumentation::Syntax syntax = "log2(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The number for which to compute the binary logarithm of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the binary logarithm of `x`.", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT log2(8);", "3"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLog2>(documentation, FunctionFactory::Case::Insensitive);
}

}
