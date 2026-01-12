#include <Functions/array/arrayIndex.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
struct NameIndexOf { static constexpr auto name = "indexOf"; };

using FunctionIndexOf = FunctionArrayIndex<IndexOfAction, NameIndexOf>;

REGISTER_FUNCTION(IndexOf)
{
    FunctionDocumentation::Description description = R"(
Returns the index of the first element with value 'x' (starting from 1) if it is in the array.
If the array does not contain the searched-for value, the function returns `0`.

Elements set to `NULL` are handled as normal values.
    )";
    FunctionDocumentation::Syntax syntax = "indexOf(arr, x)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "An array to search in for `x`.", {"Array(T)"}},
        {"x", "Value of the first matching element in `arr` for which to return an index.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the index (numbered from one) of the first `x` in `arr` if it exists. Otherwise, returns `0`.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
        {"Basic example", "SELECT indexOf([5, 4, 1, 3], 3)", "4"},
        {"Array with nulls", "SELECT indexOf([1, 3, NULL, NULL], NULL)", "3"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIndexOf>(documentation);
}
}
