#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsLogical.h>

namespace DB
{

void registerFunctionsLogical(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction*

	factory.registerFunction("and", F { return new FunctionAnd; });
	factory.registerFunction("or", 	F { return new FunctionOr; });
	factory.registerFunction("xor", F { return new FunctionXor; });
	factory.registerFunction("not", F { return new FunctionNot; });

	#undef F
}

}
