#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsVisitParam.h>

namespace DB
{

void registerFunctionsVisitParam(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction*

	factory.registerFunction("visitParamHas", 				F { return new FunctionVisitParamHas; });
	factory.registerFunction("visitParamExtractUInt", 		F { return new FunctionVisitParamExtractUInt; });
	factory.registerFunction("visitParamExtractInt", 		F { return new FunctionVisitParamExtractInt; });
	factory.registerFunction("visitParamExtractFloat", 		F { return new FunctionVisitParamExtractFloat; });
	factory.registerFunction("visitParamExtractBool", 		F { return new FunctionVisitParamExtractBool; });
	factory.registerFunction("visitParamExtractRaw", 		F { return new FunctionVisitParamExtractRaw; });
	factory.registerFunction("visitParamExtractString", 	F { return new FunctionVisitParamExtractString; });

	#undef F
}

}
