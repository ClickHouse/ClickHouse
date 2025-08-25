#include <Functions/identity.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Identity)
{
    FunctionDocumentation::Description description = R"(
This function returns the provided argument and is intended for debugging and testing.
It allows you to cancel using an index, and get the query performance of a full scan.
When the query is analyzed for possible use of an index, the analyzer ignores everything in `identity` functions.
It also disables constant folding.
)";
    FunctionDocumentation::Syntax syntax = "identity(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Input value.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the input value unchanged.", {"Any"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT identity(42)", "42"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionIdentity>(documentation);
}

REGISTER_FUNCTION(ScalarSubqueryResult)
{
    factory.registerFunction<FunctionScalarSubqueryResult>();
}

REGISTER_FUNCTION(ActionName)
{
    factory.registerFunction<FunctionActionName>();
}

}
