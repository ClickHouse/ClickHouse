#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsString.h>

namespace DB
{

void registerFunctionsString(FunctionFactory & factory)
{
	#define F [](const Context & context)

	factory.registerFunction("empty", 			F { return new FunctionEmpty; });
	factory.registerFunction("notEmpty", 		F { return new FunctionNotEmpty; });
	factory.registerFunction("length", 			F { return new FunctionLength; });
	factory.registerFunction("lengthUTF8", 		F { return new FunctionLengthUTF8; });
	factory.registerFunction("lower", 			F { return new FunctionLower; });
	factory.registerFunction("upper", 			F { return new FunctionUpper; });
	factory.registerFunction("lowerUTF8", 		F { return new FunctionLowerUTF8; });
	factory.registerFunction("upperUTF8", 		F { return new FunctionUpperUTF8; });
	factory.registerFunction("reverse", 		F { return new FunctionReverse; });
	factory.registerFunction("reverseUTF8", 	F { return new FunctionReverseUTF8; });
	factory.registerFunction("concat", 			F { return new FunctionConcat; });
	factory.registerFunction("substring", 		F { return new FunctionSubstring; });
	factory.registerFunction("substringUTF8", 	F { return new FunctionSubstringUTF8; });

	#undef F
}

}
