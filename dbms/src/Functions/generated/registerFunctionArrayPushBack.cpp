#include "registerFunctionArrayPushBack.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayPushBack(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPushBack>();
}

}
