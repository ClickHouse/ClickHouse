#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsHashing.h>


namespace DB
{

void registerFunctionsHashing(FunctionFactory & factory)
{
	#define F [](const Context & context)

	factory.registerFunction("halfMD5", 	F { return new FunctionHalfMD5; });
	factory.registerFunction("sipHash64", 	F { return new FunctionSipHash64; });
	factory.registerFunction("cityHash64", 	F { return new FunctionCityHash64; });
	factory.registerFunction("intHash32", 	F { return new FunctionIntHash32; });
	factory.registerFunction("intHash64", 	F { return new FunctionIntHash64; });

	#undef F
}

}
