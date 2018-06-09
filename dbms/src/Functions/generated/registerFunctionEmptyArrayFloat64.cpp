#include "registerFunctionEmptyArrayFloat64.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayFloat64(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayFloat64>();
}

}
