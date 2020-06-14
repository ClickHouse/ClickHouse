#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringArray.h>


namespace DB
{

void registerFunctionsStringArray(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractAll>();
    factory.registerFunction<FunctionAlphaTokens>();
    factory.registerFunction<FunctionSplitByChar>();
    factory.registerFunction<FunctionSplitByString>();
    factory.registerFunction<FunctionArrayStringConcat>();
}

}
