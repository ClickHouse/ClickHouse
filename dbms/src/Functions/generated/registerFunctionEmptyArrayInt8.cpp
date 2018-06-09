#include "registerFunctionEmptyArrayInt8.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayInt8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayInt8>();
}

}
