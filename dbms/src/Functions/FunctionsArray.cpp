#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionsArray(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction*

	factory.registerFunction("array", 				F { return new FunctionArray; });
	factory.registerFunction("arrayElement", 		F { return new FunctionArrayElement; });
	factory.registerFunction("has", 				F { return new FunctionHas; });
	factory.registerFunction("indexOf", 			F { return new FunctionIndexOf; });
	factory.registerFunction("countEqual", 			F { return new FunctionCountEqual; });
	factory.registerFunction("arrayEnumerate", 		F { return new FunctionArrayEnumerate; });
	factory.registerFunction("arrayEnumerateUniq", 	F { return new FunctionArrayEnumerateUniq; });

	#undef F
}

}
