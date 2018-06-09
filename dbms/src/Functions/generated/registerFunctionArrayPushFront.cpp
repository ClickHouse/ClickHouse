#include "registerFunctionArrayPushFront.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayPushFront(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPushFront>();
}

}
