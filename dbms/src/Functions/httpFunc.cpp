#include <Functions/FunctionFactory.h>
#include <Functions/httpFunc.h>


namespace DB
{

void registerFunctionHttpFunc(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHttpFunc>();
}

}