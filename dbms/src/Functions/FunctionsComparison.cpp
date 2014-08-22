#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsComparison.h>

namespace DB
{

void registerFunctionsComparison(FunctionFactory & factory)
{
	#define F [](const Context & context)

	factory.registerFunction("equals", 			F { return new FunctionEquals; });
	factory.registerFunction("notEquals", 		F { return new FunctionNotEquals; });
	factory.registerFunction("less", 			F { return new FunctionLess; });
	factory.registerFunction("greater", 		F { return new FunctionGreater; });
	factory.registerFunction("lessOrEquals", 	F { return new FunctionLessOrEquals; });
	factory.registerFunction("greaterOrEquals", F { return new FunctionGreaterOrEquals; });

	#undef F
}

}
