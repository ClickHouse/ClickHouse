#include "registerFunctionArrayReverse.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayReverse(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayReverse>();
}

}
