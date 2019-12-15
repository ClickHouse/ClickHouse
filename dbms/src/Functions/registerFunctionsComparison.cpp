#include "registerFunctions.h"
namespace DB
{
void registerFunctionsComparison(FunctionFactory & factory)
{
    registerFunctionEquals(factory);
    registerFunctionNotEquals(factory);
    registerFunctionLess(factory);
    registerFunctionGreater(factory);
    registerFunctionLessOrEquals(factory);
    registerFunctionGreaterOrEquals(factory);
}

}
