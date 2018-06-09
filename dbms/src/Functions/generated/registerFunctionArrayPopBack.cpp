#include <Functions/registerFunctionArrayPopBack.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayPopBack(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayPopBack>();
}

}
