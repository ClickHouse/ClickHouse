#include "arrayIndex.h"
#include <Functions/FunctionFactory.h>

namespace DB
{
struct NameCountEqual { static constexpr auto name = "countEqual"; };

using FunctionCountEqual = FunctionArrayIndex<CountEqualAction, NameCountEqual>;

void registerFunctionCountEqual(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountEqual>();
}
}
