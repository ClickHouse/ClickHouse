#include <Functions/registerFunctionHas.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionHas(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHas>();
}

}
