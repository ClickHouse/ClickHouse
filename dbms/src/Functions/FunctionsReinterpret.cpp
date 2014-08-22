#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsReinterpret.h>

namespace DB
{

void registerFunctionsReinterpret(FunctionFactory & factory)
{
	#define F [](const Context & context)

	factory.registerFunction("reinterpretAsUInt8",		F { return new FunctionReinterpretAsUInt8; });
	factory.registerFunction("reinterpretAsUInt16", 	F { return new FunctionReinterpretAsUInt16; });
	factory.registerFunction("reinterpretAsUInt32", 	F { return new FunctionReinterpretAsUInt32; });
	factory.registerFunction("reinterpretAsUInt64", 	F { return new FunctionReinterpretAsUInt64; });
	factory.registerFunction("reinterpretAsInt8", 		F { return new FunctionReinterpretAsInt8; });
	factory.registerFunction("reinterpretAsInt16", 		F { return new FunctionReinterpretAsInt16; });
	factory.registerFunction("reinterpretAsInt32", 		F { return new FunctionReinterpretAsInt32; });
	factory.registerFunction("reinterpretAsInt64", 		F { return new FunctionReinterpretAsInt64; });
	factory.registerFunction("reinterpretAsFloat32", 	F { return new FunctionReinterpretAsFloat32; });
	factory.registerFunction("reinterpretAsFloat64", 	F { return new FunctionReinterpretAsFloat64; });
	factory.registerFunction("reinterpretAsDate", 		F { return new FunctionReinterpretAsDate; });
	factory.registerFunction("reinterpretAsDateTime", 	F { return new FunctionReinterpretAsDateTime; });
	factory.registerFunction("reinterpretAsString", 	F { return new FunctionReinterpretAsString; });

	#undef F
}

}
