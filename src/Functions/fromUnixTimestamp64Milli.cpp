#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64Milli)
{
    factory.registerFunction("fromUnixTimestamp64Milli",
        [](ContextPtr context){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionFromUnixTimestamp64>(3, "fromUnixTimestamp64Milli", context->getSettingsRef().force_timezone)); });
}

}
