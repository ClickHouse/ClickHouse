#include <Functions/registerFunctionArraySlice.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArraySlice(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArraySlice>();
}

}
