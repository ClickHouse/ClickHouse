#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct TGammaName { static constexpr auto name = "tgamma"; };
using FunctionTGamma = FunctionMathUnary<UnaryFunctionVectorized<TGammaName, std::tgamma>>;

}

REGISTER_FUNCTION(TGamma)
{
    FunctionDocumentation::Description description = R"(
Returns the gamma function.
)";
    FunctionDocumentation::Syntax syntax = "tgamma(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The number for which to compute the gamma function of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the gamma function value", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT tgamma(5);", "24"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTGamma>(documentation);
}

}
