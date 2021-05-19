#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerToUnixTimestamp64Micro(FunctionFactory & factory)
{
    factory.registerFunction("toUnixTimestamp64Micro",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionToUnixTimestamp64>(6, "toUnixTimestamp64Micro")); });
}

}
