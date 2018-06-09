#include "registerFunctionArrayEnumerateUniq.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayEnumerateUniq(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayEnumerateUniq>();
}

}
