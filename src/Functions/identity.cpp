#include <Functions/identity.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Identity)
{
    FunctionDocumentation::Description description = R"(
This function returns the argument you pass to it, which is useful for debugging and testing. It lets you bypass index usage to see full scan performance instead. The query analyzer ignores anything inside identity functions when looking for indexes to use, and it also disables constant folding.
)";
    FunctionDocumentation::Syntax syntax = "identity(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Input value.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the input value unchanged.", {"Any"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT identity(42)", "42"}
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
