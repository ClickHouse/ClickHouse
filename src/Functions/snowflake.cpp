#include <Functions/FunctionSnowflake.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(DateTimeToSnowflake)
{
    factory.registerFunction("dateTimeToSnowflake",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionDateTimeToSnowflake>("dateTimeToSnowflake")); });
}

REGISTER_FUNCTION(DateTime64ToSnowflake)
{
    factory.registerFunction("dateTime64ToSnowflake",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionDateTime64ToSnowflake>("dateTime64ToSnowflake")); });
}

REGISTER_FUNCTION(SnowflakeToDateTime)
{
    factory.registerFunction("snowflakeToDateTime",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime>("snowflakeToDateTime")); });
}
REGISTER_FUNCTION(SnowflakeToDateTime64)
{
    factory.registerFunction("snowflakeToDateTime64",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime64>("snowflakeToDateTime64")); });
}

}
