#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64Nano)
{
    factory.registerFunction("fromUnixTimestamp64Nano",
        [](ContextPtr context){ return std::make_shared<FunctionFromUnixTimestamp64>(9, "fromUnixTimestamp64Nano", context); });
}

}
