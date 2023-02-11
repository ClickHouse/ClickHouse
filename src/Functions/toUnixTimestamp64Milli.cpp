#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerToUnixTimestamp64Milli(FunctionFactory & factory)
{
    factory.registerFunction("toUnixTimestamp64Milli",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionToUnixTimestamp64>(3, "toUnixTimestamp64Milli")); });
}

}
