#include <Functions/array/arrayIndex.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Common/FunctionDocumentation.h>

namespace DB
{
struct NameIndexOfAssumeSorted { static constexpr auto name = "indexOfAssumeSorted"; };

/// indexOfAssumeSorted(arr, x) - returns the index of the element x (starting with 1), if it exists in the array, or 0 if it
/// should be used when the array is sorted (applies binary search to array)
using FunctionIndexOfAssumeSorted = FunctionArrayIndex<IndexOfAssumeSorted, NameIndexOfAssumeSorted>;

REGISTER_FUNCTION(IndexOfAssumeSorted)
{
    FunctionDocumentation::Description description = R"(
Returns the index of the first element with value 'x' (starting from `1`) if it is in the array.
If the array does not contain the searched-for value, the function returns `0`.

:::note
Unlike the `indexOf` function, this function assumes that the array is sorted in
ascending order. If the array is not sorted, results are undefined.
:::
    )";
    FunctionDocumentation::Syntax syntax = "indexOfAssumeSorted(arr, x)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "A sorted array to search.", {"Array(T)"}},
        {"x", "Value of the first matching element in sorted `arr` for which to return an index.", {"UInt64"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the index (numbered from one) of the first `x` in `arr` if it exists. Otherwise, returns `0`.", {"UInt64"}};
    FunctionDocumentation::Examples example = {{"Basic example", "SELECT indexOfAssumeSorted([1, 3, 3, 3, 4, 4, 5], 4)", "5"}};
    FunctionDocumentation::IntroducedIn introduced_in = {24, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};

    factory.registerFunction<FunctionIndexOfAssumeSorted>(documentation);
}
}
