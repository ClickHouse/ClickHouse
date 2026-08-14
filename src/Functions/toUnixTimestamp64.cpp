#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Second)
{
    factory.registerFunction("toUnixTimestamp64Second",
        [](ContextPtr){ return std::make_shared<FunctionToUnixTimestamp64>(0, "toUnixTimestamp64Second"); });
}

}
