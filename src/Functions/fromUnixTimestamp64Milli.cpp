#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerFromUnixTimestamp64Milli(FunctionFactory & factory)
{
    factory.registerFunction("fromUnixTimestamp64Milli",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionFromUnixTimestamp64>(3, "fromUnixTimestamp64Milli")); });
}

}
