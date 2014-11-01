#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionsArray(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction *

	factory.registerFunction("array", 				F { return new FunctionArray; });
	factory.registerFunction("arrayElement", 		F { return new FunctionArrayElement; });
	factory.registerFunction("has", 				F { return new FunctionHas; });
	factory.registerFunction("indexOf", 			F { return new FunctionIndexOf; });
	factory.registerFunction("countEqual", 			F { return new FunctionCountEqual; });
	factory.registerFunction("arrayEnumerate", 		F { return new FunctionArrayEnumerate; });
	factory.registerFunction("arrayEnumerateUniq", 	F { return new FunctionArrayEnumerateUniq; });
	factory.registerFunction("emptyArrayUInt8",		F { return new FunctionEmptyArrayUInt8; });
	factory.registerFunction("emptyArrayUInt16",	F { return new FunctionEmptyArrayUInt16; });
	factory.registerFunction("emptyArrayUInt32",	F { return new FunctionEmptyArrayUInt32; });
	factory.registerFunction("emptyArrayUInt64",	F { return new FunctionEmptyArrayUInt64; });
	factory.registerFunction("emptyArrayInt8",		F { return new FunctionEmptyArrayInt8; });
	factory.registerFunction("emptyArrayInt16",		F { return new FunctionEmptyArrayInt16; });
	factory.registerFunction("emptyArrayInt32",		F { return new FunctionEmptyArrayInt32; });
	factory.registerFunction("emptyArrayInt64",		F { return new FunctionEmptyArrayInt64; });
	factory.registerFunction("emptyArrayFloat32",	F { return new FunctionEmptyArrayFloat32; });
	factory.registerFunction("emptyArrayFloat64",	F { return new FunctionEmptyArrayFloat64; });
	factory.registerFunction("emptyArrayDate",		F { return new FunctionEmptyArrayDate; });
	factory.registerFunction("emptyArrayDateTime",	F { return new FunctionEmptyArrayDateTime; });
	factory.registerFunction("emptyArrayString",	F { return new FunctionEmptyArrayString; });


	#undef F
}

}
