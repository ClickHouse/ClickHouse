#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

struct AsinName { static constexpr auto name = "asin"; };
using FunctionAsin = FunctionMathUnary<UnaryFunctionVectorized<AsinName, asin>>;

}

REGISTER_FUNCTION(Asin)
{
    FunctionDocumentation::Description description = R"(
Calculates the arcsine of the provided argument.
For arguments in the range `[-1, 1]` it returns the value in the range of `[-pi() / 2, pi() / 2]`.
    )";
    FunctionDocumentation::Syntax syntax = "asin(x)";
    FunctionDocumentation::Arguments argument = {
        {"x", "Argument for which to calculate arcsine of.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the arcsine value of the provided argument `x`", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {"inverse", "SELECT asin(1.0) = pi() / 2, sin(asin(1)), asin(sin(1))", "1 1 1"},
    {"float32", "SELECT toTypeName(asin(1.0::Float32))", "Float64"},
    {"nan", "SELECT asin(1.1), asin(-2), asin(inf), asin(nan)", "nan nan nan nan"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, argument, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionAsin>(documentation, FunctionFactory::Case::Insensitive);
}

}
