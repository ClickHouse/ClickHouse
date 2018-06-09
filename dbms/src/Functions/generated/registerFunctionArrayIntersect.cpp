#include "registerFunctionArrayIntersect.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayIntersect(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayIntersect>();
}

}
