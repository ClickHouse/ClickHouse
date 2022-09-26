#include "FunctionHashID.h"
#include <Functions/FunctionFactory.h>

namespace DB
{

void registerFunctionHashID(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHashID>();
}

}
