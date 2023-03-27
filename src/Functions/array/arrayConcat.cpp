#include <Functions/array/arrayConcat.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ArrayConcat)
{
    factory.registerFunction<FunctionArrayConcat>();
}

}
