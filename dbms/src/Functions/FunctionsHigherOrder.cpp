#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsHigherOrder.h>

namespace DB
{

void registerFunctionsHigherOrder(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction*

	factory.registerFunction("arrayMap", 		F { return new FunctionArrayMap; });
	factory.registerFunction("arrayFilter", 	F { return new FunctionArrayFilter; });
	factory.registerFunction("arrayCount", 		F { return new FunctionArrayCount; });
	factory.registerFunction("arrayExists", 	F { return new FunctionArrayExists; });
	factory.registerFunction("arrayAll", 		F { return new FunctionArrayAll; });
	factory.registerFunction("arraySum", 		F { return new FunctionArraySum; });

	#undef F
}

}
