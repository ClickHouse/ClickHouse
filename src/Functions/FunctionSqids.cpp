#include "FunctionSqids.h"
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Sqids)
{
    factory.registerFunction<FunctionSqids>();
}

}
