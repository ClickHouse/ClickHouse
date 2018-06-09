#include "registerFunctionEmptyArrayInt16.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayInt16(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayInt16>();
}

}
