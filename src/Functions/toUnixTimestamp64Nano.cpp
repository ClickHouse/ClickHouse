#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerToUnixTimestamp64Nano(FunctionFactory & factory)
{
    factory.registerFunction("toUnixTimestamp64Nano",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionToUnixTimestamp64>(9, "toUnixTimestamp64Nano")); });
}

}
