#include <Functions/registerFunctionArrayPushFront.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayPushFront(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPushFront>();
}

}
