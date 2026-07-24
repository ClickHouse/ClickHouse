#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Micro)
{
    factory.registerFunction("toUnixTimestamp64Micro",
        [](ContextPtr){ return std::make_shared<FunctionToUnixTimestamp64>(6, "toUnixTimestamp64Micro"); });
}

}
