#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsVisitParam.h>

namespace DB
{

void registerFunctionsVisitParam(FunctionFactory & factory)
{
	factory.registerFunction<FunctionVisitParamHas>();
	factory.registerFunction<FunctionVisitParamExtractUInt>();
	factory.registerFunction<FunctionVisitParamExtractInt>();
	factory.registerFunction<FunctionVisitParamExtractFloat>();
	factory.registerFunction<FunctionVisitParamExtractBool>();
	factory.registerFunction<FunctionVisitParamExtractRaw>();
	factory.registerFunction<FunctionVisitParamExtractString>();
}

}
