#include <Functions/registerFunctionArrayIntersect.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayIntersect(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayIntersect>();
}

}
