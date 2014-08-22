#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsStringArray.h>

namespace DB
{

void registerFunctionsStringArray(FunctionFactory & factory)
{
	#define F [](const Context & context)

	factory.registerFunction("extractAll", 		F { return new FunctionExtractAll; });
	factory.registerFunction("alphaTokens", 	F { return new FunctionAlphaTokens; });
	factory.registerFunction("splitByChar", 	F { return new FunctionSplitByChar; });
	factory.registerFunction("splitByString", 	F { return new FunctionSplitByString; });

	#undef F
}

}
