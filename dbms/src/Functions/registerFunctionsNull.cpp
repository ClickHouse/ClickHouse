#include "registerFunctions.h"
namespace DB
{
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
