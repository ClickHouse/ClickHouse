#include <Functions/registerFunctionArrayElement.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayElement(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayElement>();
}

}
