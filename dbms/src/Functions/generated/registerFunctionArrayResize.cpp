#include <Functions/registerFunctionArrayResize.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayResize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayResize>();
}

}
