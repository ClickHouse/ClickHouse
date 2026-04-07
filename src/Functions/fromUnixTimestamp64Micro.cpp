#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64Micro)
{
    factory.registerFunction("fromUnixTimestamp64Micro",
        [](ContextPtr context){ return std::make_shared<FunctionFromUnixTimestamp64>(6, "fromUnixTimestamp64Micro", context); });
}

}
