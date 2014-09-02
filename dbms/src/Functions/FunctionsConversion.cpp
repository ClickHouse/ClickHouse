#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsConversion.h>

namespace DB
{

void registerFunctionsConversion(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction*

	factory.registerFunction("toUInt8", 		F { return new FunctionToUInt8; });
	factory.registerFunction("toUInt16", 		F { return new FunctionToUInt16; });
	factory.registerFunction("toUInt32", 		F { return new FunctionToUInt32; });
	factory.registerFunction("toUInt64", 		F { return new FunctionToUInt64; });
	factory.registerFunction("toInt8", 			F { return new FunctionToInt8; });
	factory.registerFunction("toInt16", 		F { return new FunctionToInt16; });
	factory.registerFunction("toInt32", 		F { return new FunctionToInt32; });
	factory.registerFunction("toInt64", 		F { return new FunctionToInt64; });
	factory.registerFunction("toFloat32", 		F { return new FunctionToFloat32; });
	factory.registerFunction("toFloat64", 		F { return new FunctionToFloat64; });
	factory.registerFunction("toDate", 			F { return new FunctionToDate; });
	factory.registerFunction("toDateTime", 		F { return new FunctionToDateTime; });
	factory.registerFunction("toString", 		F { return new FunctionToString; });
	factory.registerFunction("toFixedString", 	F { return new FunctionToFixedString; });

	#undef F
}

}
