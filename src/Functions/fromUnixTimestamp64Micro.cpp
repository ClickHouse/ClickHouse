#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerFromUnixTimestamp64Micro(FunctionFactory & factory)
{
    factory.registerFunction("fromUnixTimestamp64Micro",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionFromUnixTimestamp64>(6, "fromUnixTimestamp64Micro")); });
}

}
