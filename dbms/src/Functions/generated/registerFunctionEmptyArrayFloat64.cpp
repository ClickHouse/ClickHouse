#include <Functions/registerFunctionEmptyArrayFloat64.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionEmptyArrayFloat64(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmptyArrayFloat64>();
}

}
