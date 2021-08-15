#include <Functions/FunctionSnowflake.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerSnowflakeToDateTime64(FunctionFactory & factory)
{
    factory.registerFunction("snowflakeToDateTime64",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime64>("snowflakeToDateTime64")); });
}

}
