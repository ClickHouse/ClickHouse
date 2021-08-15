#include <Functions/FunctionSnowflake.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerDateTime64ToSnowflake(FunctionFactory & factory)
{
    factory.registerFunction("dateTime64ToSnowflake",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionDateTime64ToSnowflake>("dateTime64ToSnowflake")); });
}

}
