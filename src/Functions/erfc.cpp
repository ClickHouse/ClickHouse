#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct ErfcName { static constexpr auto name = "erfc"; };
using FunctionErfc = FunctionMathUnary<UnaryFunctionVectorized<ErfcName, std::erfc>>;

}

REGISTER_FUNCTION(Erfc)
{
    FunctionDocumentation::Description description = R"(
Returns a number close to `1-erf(x)` without loss of precision for large `x` values.
)";
    FunctionDocumentation::Syntax syntax = "erfc(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The value for which to find the error function value.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the complementary error function value", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT erfc(0);", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionErfc>(documentation);
}

}
