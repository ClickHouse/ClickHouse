#include "arrayIndex.h"
#include <Functions/FunctionFactory.h>

namespace DB
{
struct NameIndexOf { static constexpr auto name = "indexOf"; };

/// indexOf(arr, x) - returns the index of the element x (starting with 1), if it exists in the array, or 0 if it
/// doesn't.
using FunctionIndexOf = FunctionArrayIndex<IndexOfAction, NameIndexOf>;

REGISTER_FUNCTION(IndexOf)
{
    factory.registerFunction<FunctionIndexOf>();
}
}
