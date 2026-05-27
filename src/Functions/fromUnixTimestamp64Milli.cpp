#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64Milli)
{
    factory.registerFunction("fromUnixTimestamp64Milli",
        [](ContextPtr context){ return std::make_shared<FunctionFromUnixTimestamp64>(3, "fromUnixTimestamp64Milli", context); });
}

}
