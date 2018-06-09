#include <Functions/registerFunctionCountEqual.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionCountEqual(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountEqual>();
}

}
