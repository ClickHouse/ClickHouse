#define ARRAY_EXCEPT_CPP_INCLUDE
#include "arrayIntersect.cpp"

namespace DB
{

using ArrayExcept = FunctionArrayIntersect<ArrayModeExcept>;

REGISTER_FUNCTION(ArrayExcept)
{
    FunctionDocumentation::Description except_description = "Takes multiple arrays and returns an array with elements that are present in the first array but not in any subsequent arrays. The result contains only unique values.";
    FunctionDocumentation::Syntax except_syntax = "arrayExcept(arr1, arr2, ..., arrN)";
    FunctionDocumentation::Arguments except_argument = {{"arrN", "N arrays. Returns elements from first array not in others. [`Array(T)`](/sql-reference/data-types/array)."}};
    FunctionDocumentation::ReturnedValue except_returned_value = {"Returns an array with distinct elements from first array not present in other arrays", {"Array(T)"}};
    FunctionDocumentation::Examples except_example = {{"Usage example",
R"(SELECT
arrayExcept([1, 2, 3], [2, 3]) AS basic_example,
arrayExcept([1, 2, 3], [2, 3], [4]) AS multiple_arrays,
arrayExcept([1, 2, NULL], [2, NULL]) AS with_null
)", R"(
┌─basic_example─┬─multiple_arrays─┬─with_null─┐
│ [1]           │ [1]             │ [1]       │
└───────────────┴─────────────────┴───────────┘
)"}};
    FunctionDocumentation::IntroducedIn except_introduced_in = {25, 5};
    FunctionDocumentation::Category except_category = FunctionDocumentation::Category::Array;
    FunctionDocumentation except_documentation = {except_description, except_syntax, except_argument, except_returned_value, except_example, except_introduced_in, except_category};

    factory.registerFunction<ArrayExcept>(except_documentation);
}

}
