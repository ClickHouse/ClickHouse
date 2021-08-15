#include <Functions/FunctionSnowflake.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerSnowflakeToDateTime(FunctionFactory & factory)
{
    factory.registerFunction("snowflakeToDateTime",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime>("snowflakeToDateTime")); });
}

}
