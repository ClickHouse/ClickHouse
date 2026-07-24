#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Milli)
{
    factory.registerFunction("toUnixTimestamp64Milli",
        [](ContextPtr){ return std::make_shared<FunctionToUnixTimestamp64>(3, "toUnixTimestamp64Milli"); });
}

}
