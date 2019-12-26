#include "registerFunctions.h"
namespace DB
{
void registerFunctionsVisitParam(FunctionFactory & factory)
{
    registerFunctionVisitParamHas(factory);
    registerFunctionVisitParamExtractUInt(factory);
    registerFunctionVisitParamExtractInt(factory);
    registerFunctionVisitParamExtractFloat(factory);
    registerFunctionVisitParamExtractBool(factory);
    registerFunctionVisitParamExtractRaw(factory);
    registerFunctionVisitParamExtractString(factory);
}

}
