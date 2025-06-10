#include "arrayIndex.h"
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include "Common/FunctionDocumentation.h"

namespace DB
{
struct NameIndexOfAssumeSorted { static constexpr auto name = "indexOfAssumeSorted"; };

/// indexOfAssumeSorted(arr, x) - returns the index of the element x (starting with 1), if it exists in the array, or 0 if it
/// should be used when the array is sorted (applies binary search to array)
using FunctionIndexOfAssumeSorted = FunctionArrayIndex<IndexOfAssumeSorted, NameIndexOfAssumeSorted>;

REGISTER_FUNCTION(IndexOfAssumeSorted)
{
    factory.registerFunction<FunctionIndexOfAssumeSorted>(FunctionDocumentation{
        .description = R"(
The function finds the position of the first occurrence of element X in the array.
Indexing from one.
The function can be used when the internal array type is not Nullable and the array is sorted in non-decreasing order.
If the array type is Nullable, the 'indexOf' function will be used.
The binary search algorithm is used for the search.
For more details, see [https://en.wikipedia.org/wiki/Binary_search]
For an unsorted array, the behavior is undefined.
)",
        .examples = {{.name = "", .query = "SELECT indexOfAssumeSorted([1, 2, 2, 2, 3, 3, 3, 4], 3) FROM test_table;", .result=""}}});
}
}
