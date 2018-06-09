#include "registerFunctionArraySlice.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArray.h"

namespace DB
{

void registerFunctionArraySlice(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArraySlice>();
}

}
