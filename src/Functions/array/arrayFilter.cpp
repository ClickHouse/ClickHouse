#include <Functions/array/arrayFilter.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ArrayFilter)
{
    factory.registerFunction<FunctionArrayFilter>();
}

}
