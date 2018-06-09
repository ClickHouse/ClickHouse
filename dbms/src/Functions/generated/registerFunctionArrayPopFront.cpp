#include <Functions/registerFunctionArrayPopFront.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayPopFront(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPopFront>();
}

}
