#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsJson.h>


namespace DB
{
void registerFunctionsJson(FunctionFactory & factory)
{
    factory.registerFunction<FunctionJson<AnyResultContainer>>();
    factory.registerFunction<FunctionJson<CountResultContainer>>();
    factory.registerFunction<FunctionJson<AllResultContainer>>();
    factory.registerFunction<FunctionJson<AnyArrayResultContainer>>();
    factory.registerFunction<FunctionJson<AllArraysResultContainer>>();
}

}
