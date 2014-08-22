#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsFormatting.h>

namespace DB
{

void registerFunctionsFormatting(FunctionFactory & factory)
{
	#define F [](const Context & context)

	factory.registerFunction("bitmaskToList", 	F { return new FunctionBitmaskToList; });

	#undef F
}

}
