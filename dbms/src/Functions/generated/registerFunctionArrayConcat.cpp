#include <Functions/registerFunctionArrayConcat.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayConcat(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayConcat>();
}

}
