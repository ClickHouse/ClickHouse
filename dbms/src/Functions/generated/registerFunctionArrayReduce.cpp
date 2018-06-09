#include <Functions/registerFunctionArrayReduce.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionArrayReduce(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayReduce>();
}

}
