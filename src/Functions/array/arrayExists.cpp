#include <Functions/array/arrayExists.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ArrayExists)
{
    factory.registerFunction<FunctionArrayExists>();
}

}
