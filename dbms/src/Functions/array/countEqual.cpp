#include "arrayIndex.h"
#include <Functions/FunctionFactory.h>
#include "registerFunctionsArray.h"


namespace DB
{

struct NameCountEqual { static constexpr auto name = "countEqual"; };

using FunctionCountEqual = FunctionArrayIndex<IndexCount, NameCountEqual>;

void registerFunctionCountEqual(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountEqual>();
}


}
