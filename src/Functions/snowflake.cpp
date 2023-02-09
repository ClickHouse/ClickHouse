#include <Functions/FunctionSnowflake.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerDateTimeToSnowflake(FunctionFactory & factory)
{
    factory.registerFunction("dateTimeToSnowflake",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionDateTimeToSnowflake>("dateTimeToSnowflake")); });
}

void registerDateTime64ToSnowflake(FunctionFactory & factory)
{
    factory.registerFunction("dateTime64ToSnowflake",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionDateTime64ToSnowflake>("dateTime64ToSnowflake")); });
}

void registerSnowflakeToDateTime(FunctionFactory & factory)
{
    factory.registerFunction("snowflakeToDateTime",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime>("snowflakeToDateTime")); });
}
void registerSnowflakeToDateTime64(FunctionFactory & factory)
{
    factory.registerFunction("snowflakeToDateTime64",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime64>("snowflakeToDateTime64")); });
}

}
