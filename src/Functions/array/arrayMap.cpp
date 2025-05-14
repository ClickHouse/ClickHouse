#include <Functions/array/arrayMap.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ArrayMap)
{
    FunctionDocumentation::Description description = R"(
Returns an array obtained from the original arrays by application of `func(arr1[i], ..., arrN[i])` for each element.
Arrays `arr1` ... `arrN` must have the same number of elements.

`arrayMap` is a [higher-order function](/sql-reference/functions/overview#higher-order-functions). You must pass a lambda function to it as the first argument, and it can't be omitted.
)";
    FunctionDocumentation::Syntax syntax = "arrayMap(func, arr1 [, ..., arrN])";
    FunctionDocumentation::Arguments arguments = {
        {"func", "Function to apply to each element of the array(s). Optional. [Lambda function](/sql-reference/functions/overview#arrow-operator-and-lambda)"},
        {"arr1 [, ..., arrN]", "N arrays to apply `f` to. [Array(T)](/sql-reference/data-types/array)."},
    };
    FunctionDocumentation::ReturnedValue returned_value = "Returns an array with elements the results of applying `f` to the original array. [`Array(T)`](/sql-reference/data-types/array)";
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;", "[3,4,5]"},
        {"Creating a tuple of elements from different arrays", "SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) AS res", "[(1,4),(2,5),(3,6)]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayMap>(documentation);
}

}
