#include "arrayIndex.h"
#include <Functions/FunctionFactory.h>

namespace DB
{
struct NameHas { static constexpr auto name = "has"; };

/// has(arr, x) - whether there is an element x in the array.
using FunctionHas = FunctionArrayIndex<HasAction, NameHas>;

void registerFunctionHas(FunctionFactory & factory) { factory.registerFunction<FunctionHas>(); }
}
