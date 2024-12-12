#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64)
{
    factory.registerFunction("fromUnixTimestamp64Second",
        [](ContextPtr context){ return std::make_shared<FunctionFromUnixTimestamp64>(0, "fromUnixTimestamp64Second", context); });
}

}
