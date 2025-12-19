#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

struct AcosName { static constexpr auto name = "acos"; };
using FunctionAcos = FunctionMathUnary<UnaryFunctionVectorized<AcosName, acos>>;

}

REGISTER_FUNCTION(Acos)
{
    FunctionDocumentation::Description description = R"(
Returns the arc cosine of the argument.
)";
    FunctionDocumentation::Syntax syntax = "acos(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The value for which to find arc cosine of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the arc cosine of x", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT acos(0.5);", "1.0471975511965979"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAcos>(documentation, FunctionFactory::Case::Insensitive);
}

}
