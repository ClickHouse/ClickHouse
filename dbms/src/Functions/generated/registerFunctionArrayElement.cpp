#include "registerFunctionArrayElement.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayElement(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayElement>();
}

}
