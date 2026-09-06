#include <Functions/FunctionFactory.h>
#include <Functions/materialize.h>


namespace DB
{

REGISTER_FUNCTION(Materialize)
{
    FunctionDocumentation::Description description = R"(
Turns a constant into a full column containing a single value.
Full columns and constants are represented differently in memory.
Functions usually execute different code for normal and constant arguments, although the result should typically be the same.
This function can be used to debug this behavior.
)";
    FunctionDocumentation::Syntax syntax = "materialize(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "A constant.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a full column containing the constant value.", {"Any"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
-- In the example below the `countMatches` function expects a constant second argument.
-- This behaviour can be debugged by using the `materialize` function to turn a constant into a full column,
-- verifying that the function throws an error for a non-constant argument.

SELECT countMatches('foobarfoo', 'foo');
        )",
        R"(
2
        )"
    },
    {
        "Turning a constant into a full column",
        R"(
SELECT countMatches('foobarfoo', materialize('foo'));
        )",
        R"(
Received exception:
Code: 44. DB::Exception: A value of illegal type was provided as 2nd argument 'pattern' to function 'countMatches'. Expected: constant String, got: String. (ILLEGAL_COLUMN)
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionMaterialize<true>>(documentation);
}

}
