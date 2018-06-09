#include "registerFunctionCountEqual.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionCountEqual(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountEqual>();
}

}
