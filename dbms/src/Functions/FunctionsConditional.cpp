#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsConditional.h>

namespace DB
{

void registerFunctionsConditional(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction*

	factory.registerFunction("if", F { return new FunctionIf; });

	#undef F
}

}
