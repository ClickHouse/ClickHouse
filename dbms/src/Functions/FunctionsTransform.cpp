#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsTransform.h>

namespace DB
{

void registerFunctionsTransform(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTransform>();
}

}
