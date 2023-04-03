#include <Functions/array/arrayAll.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ArrayAll)
{
    factory.registerFunction<FunctionArrayAll>();
}

}
