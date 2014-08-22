#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsRandom.h>

namespace DB
{

void registerFunctionsRandom(FunctionFactory & factory)
{
	#define F [](const Context & context)

	factory.registerFunction("rand", 	F { return new FunctionRand; });
	factory.registerFunction("rand64", 	F { return new FunctionRand64; });

	#undef F
}

}
