#include <Functions/arrayIndex.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

struct NameCountEqual { static constexpr auto name = "countEqual"; };

using FunctionCountEqual = FunctionArrayIndex<IndexCount, NameCountEqual>;

void registerFunctionCountEqual(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountEqual>();
}


}
