#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsFindCluster.h>

namespace DB
{

void registerFunctionsFindCluster(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFindClusterIndex>();
    factory.registerFunction<FunctionFindClusterValue>();
}

}
