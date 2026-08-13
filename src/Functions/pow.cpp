#include <Functions/FunctionMathBinaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct PowName { static constexpr auto name = "pow"; };
using FunctionPow = FunctionMathBinaryFloat64<BinaryFunctionVectorized<PowName, pow>>;

}

REGISTER_FUNCTION(Pow)
{
    FunctionDocumentation::Description description = R"(
Returns x raised to the power of y.
)";
    FunctionDocumentation::Syntax syntax = "pow(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The base.", {"(U)Int8/16/32/64", "Float*", "Decimal*"}},
        {"y", "The exponent.", {"(U)Int8/16/32/64", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns x^y", {"Float64"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT pow(2, 3);", "8"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPow>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("power", "pow", FunctionFactory::Case::Insensitive);
}

}
