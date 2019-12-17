#include "registerFunctions.h"
namespace DB
{
void registerFunctionsConditional(FunctionFactory & factory)
{
    registerFunctionIf(factory);
    registerFunctionMultiIf(factory);
    registerFunctionCaseWithExpression(factory);
}

}
