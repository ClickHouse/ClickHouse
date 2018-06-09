#include "registerFunctionArrayPopFront.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayPopFront(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPopFront>();
}

}
