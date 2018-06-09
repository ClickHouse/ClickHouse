#include "registerFunctionIndexOf.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionIndexOf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIndexOf>();
}

}
