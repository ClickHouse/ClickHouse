#include "registerFunctionRange.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionRange(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRange>();
}

}
