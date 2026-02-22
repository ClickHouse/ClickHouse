#include <Functions/array/arrayMap.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ArrayMap)
{
    factory.registerFunction<FunctionArrayMap>();
}

}
