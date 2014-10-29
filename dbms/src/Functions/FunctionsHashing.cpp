#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsHashing.h>


namespace DB
{

void registerFunctionsHashing(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction *

	factory.registerFunction("halfMD5", 	F { return new FunctionHalfMD5; });
	factory.registerFunction("MD5",			F { return new FunctionMD5; });
	factory.registerFunction("SHA1",		F { return new FunctionSHA1; });
	factory.registerFunction("SHA224",		F { return new FunctionSHA224; });
	factory.registerFunction("SHA256",		F { return new FunctionSHA256; });
	factory.registerFunction("sipHash64", 	F { return new FunctionSipHash64; });
	factory.registerFunction("sipHash128",	F { return new FunctionSipHash128; });
	factory.registerFunction("cityHash64", 	F { return new FunctionCityHash64; });
	factory.registerFunction("intHash32", 	F { return new FunctionIntHash32; });
	factory.registerFunction("intHash64", 	F { return new FunctionIntHash64; });

	#undef F
}

}
