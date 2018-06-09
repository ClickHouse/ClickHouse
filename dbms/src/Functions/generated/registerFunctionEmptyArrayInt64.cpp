#include <Functions/registerFunctionEmptyArrayInt64.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionEmptyArrayInt64(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayInt64>();
}

}
