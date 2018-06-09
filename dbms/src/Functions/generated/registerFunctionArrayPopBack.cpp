#include "registerFunctionArrayPopBack.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayPopBack(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPopBack>();
}

}
