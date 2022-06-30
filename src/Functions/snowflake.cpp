#include <Functions/FunctionSnowflake.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

static COLD_INIT void registerFunctionsSnowflake(FunctionFactory & factory)
{
    factory.registerFunction("dateTimeToSnowflake",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionDateTimeToSnowflake>("dateTimeToSnowflake")); });

    factory.registerFunction("dateTime64ToSnowflake",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionDateTime64ToSnowflake>("dateTime64ToSnowflake")); });

    factory.registerFunction("snowflakeToDateTime",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime>("snowflakeToDateTime")); });

    factory.registerFunction("snowflakeToDateTime64",
        [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionSnowflakeToDateTime64>("snowflakeToDateTime64")); });
}

FUNCTION_REGISTER(registerFunctionsSnowflake);

}
