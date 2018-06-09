#include <Functions/registerFunctionArrayPushBack.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayPushBack(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPushBack>();
}

}
