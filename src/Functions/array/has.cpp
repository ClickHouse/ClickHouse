#include "arrayIndex.h"
#include <Functions/FunctionFactory.h>

namespace DB
{
struct NameHas { static constexpr auto name = "has"; };

/// has(arr, x) - whether there is an element x in the array.
using FunctionHas = FunctionArrayIndex<HasAction, NameHas>;

REGISTER_FUNCTION(Has) { factory.registerFunction<FunctionHas>(); }

FunctionOverloadResolverPtr createInternalFunctionHasOverloadResolver()
{
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionHas>());
}

}
