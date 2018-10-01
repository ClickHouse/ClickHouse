#include <Functions/FunctionFactory.h>

namespace DB
{

void registerFunctionIsNull(FunctionFactory & factory);
void registerFunctionIsNotNull(FunctionFactory & factory);
void registerFunctionCoalesce(FunctionFactory & factory);
void registerFunctionIfNull(FunctionFactory & factory);
void registerFunctionNullIf(FunctionFactory & factory);
void registerFunctionAssumeNotNull(FunctionFactory & factory);
void registerFunctionToNullable(FunctionFactory & factory);

void registerFunctionsNull(FunctionFactory & factory)
{
    registerFunctionIsNull(factory);
    registerFunctionIsNotNull(factory);
    registerFunctionCoalesce(factory);
    registerFunctionIfNull(factory);
    registerFunctionNullIf(factory);
    registerFunctionAssumeNotNull(factory);
    registerFunctionToNullable(factory);
}

}

