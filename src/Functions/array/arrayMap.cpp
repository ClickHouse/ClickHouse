#include <Functions/array/arrayMap.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ArrayMap)
{
    FunctionDocumentation::Description description = R"(
Returns an array obtained from the original arrays by applying a lambda function to each element.
)";
    FunctionDocumentation::Syntax syntax = "arrayMap(func, arr)";
    FunctionDocumentation::Arguments arguments = {
        {"func", "A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"arr", "N arrays to process.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array from the lambda results", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;", "[3, 4, 5]"},
        {"Creating a tuple of elements from different arrays", "SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) AS res", "[(1, 4),(2, 5),(3, 6)]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayMap>(documentation);
}

}
