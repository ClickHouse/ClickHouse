#include "FunctionHashID.h"
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(HashID)
{
    factory.registerFunction<FunctionHashID>();
}

}
