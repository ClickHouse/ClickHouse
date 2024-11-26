#include "arrayIndex.h"
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB
{
struct NameIndexOfAssumeSorted { static constexpr auto name = "indexOfAssumeSorted"; };

/// indexOfAssumeSorted(arr, x) - returns the index of the element x (starting with 1), if it exists in the array, or 0 if it
/// should be used when the array is sorted (applies binary search to array)
using FunctionIndexOfAssumeSorted = FunctionArrayIndex<IndexOfAssumeSorted, NameIndexOfAssumeSorted>;

REGISTER_FUNCTION(IndexOfAssumeSorted) { factory.registerFunction<FunctionIndexOfAssumeSorted>(); }
}
