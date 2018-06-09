#include "registerFunctionArrayResize.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArrayResize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayResize>();
}

}
