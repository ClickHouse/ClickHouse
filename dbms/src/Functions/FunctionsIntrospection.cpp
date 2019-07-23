#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsIntrospection.h>

namespace DB
{

void registerFunctionsIntrospection(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSymbolizeTrace>();
}

}
