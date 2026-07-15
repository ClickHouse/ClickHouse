#include <Functions/runningDifference.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(RunningDifferenceStartingWithFirstValue)
{
    FunctionDocumentation::Description description = R"(
Calculates the difference between consecutive row values in a data block, but unlike [`runningDifference`](#runningDifference), it returns the actual value of the first row instead of `0`.

:::warning Deprecated
Only returns differences inside the currently processed data block.
Because of this error-prone behavior, the function is deprecated.
It is advised to use [window functions](/sql-reference/window-functions) instead.

You can use setting `allow_deprecated_error_prone_window_functions` to allow usage of this function.
:::
)";
    FunctionDocumentation::Syntax syntax = "runningDifferenceStartingWithFirstValue(x)";
    FunctionDocumentation::Arguments arguments = {{"x", "Column for which to calculate the running difference.", {"Any"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the difference between consecutive values, with the first row's value for the first row.", {"Any"}};
    FunctionDocumentation::Examples examples = {{"Usage example",
        R"(
SELECT
    number,
    runningDifferenceStartingWithFirstValue(number) AS diff
FROM numbers(5);
        )",
        R"(
┌─number─┬─diff─┐
│      0 │    0 │
│      1 │    1 │
│      2 │    1 │
│      3 │    1 │
│      4 │    1 │
└────────┴──────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRunningDifferenceImpl<false>>(documentation);
}

}
