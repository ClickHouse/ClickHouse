#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsRound.h>

namespace DB
{

void registerFunctionsRound(FunctionFactory & factory)
{
	factory.registerFunction<FunctionRoundToExp2>();
	factory.registerFunction<FunctionRoundDuration>();
	factory.registerFunction<FunctionRoundAge>();
}

}
