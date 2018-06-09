#include <Functions/registerFunctionArrayReverse.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayReverse(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayReverse>();
}

}
