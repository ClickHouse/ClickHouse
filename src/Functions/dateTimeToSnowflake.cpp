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

}
