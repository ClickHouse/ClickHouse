#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsCoding.h>

namespace DB
{

void registerFunctionsCoding(FunctionFactory & factory)
{
	#define F [](const Context & context)

	factory.registerFunction("toStringCutToZero", 	F { return new FunctionToStringCutToZero; });
	factory.registerFunction("IPv4NumToString", 	F { return new FunctionIPv4NumToString; });
	factory.registerFunction("IPv4StringToNum", 	F { return new FunctionIPv4StringToNum; });
	factory.registerFunction("hex", 				F { return new FunctionHex; });
	factory.registerFunction("unhex", 				F { return new FunctionUnhex; });
	factory.registerFunction("bitmaskToArray",		F { return new FunctionBitmaskToArray; });

	#undef F
}

}
