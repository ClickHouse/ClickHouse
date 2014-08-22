#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsRound.h>

namespace DB
{

void registerFunctionsRound(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction*

	factory.registerFunction("roundToExp2", 	F { return new FunctionRoundToExp2; });
	factory.registerFunction("roundDuration", 	F { return new FunctionRoundDuration; });
	factory.registerFunction("roundAge", 		F { return new FunctionRoundAge; });

	#undef F
}

}
