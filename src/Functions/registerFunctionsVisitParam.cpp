namespace DB
{

class FunctionFactory;

void registerFunctionVisitParamHas(FunctionFactory & factory);
void registerFunctionVisitParamExtractUInt(FunctionFactory & factory);
void registerFunctionVisitParamExtractInt(FunctionFactory & factory);
void registerFunctionVisitParamExtractFloat(FunctionFactory & factory);
void registerFunctionVisitParamExtractBool(FunctionFactory & factory);
void registerFunctionVisitParamExtractRaw(FunctionFactory & factory);
void registerFunctionVisitParamExtractString(FunctionFactory & factory);

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
