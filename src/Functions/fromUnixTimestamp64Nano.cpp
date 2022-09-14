#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64Nano)
{
    factory.registerFunction("fromUnixTimestamp64Nano",
        [](ContextPtr context){ return std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionFromUnixTimestamp64>(9, "fromUnixTimestamp64Nano", context->getSettingsRef().default_user_timezone)); });
}

}
