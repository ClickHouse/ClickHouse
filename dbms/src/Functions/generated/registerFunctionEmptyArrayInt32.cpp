#include "registerFunctionEmptyArrayInt32.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayInt32(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayInt32>();
}

}
