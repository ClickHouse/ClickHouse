#include "registerFunctionEmptyArrayToSingle.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionEmptyArrayToSingle(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayToSingle>();
}

}
