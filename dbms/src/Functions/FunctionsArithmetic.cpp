#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionsArithmetic(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction*

	factory.registerFunction("plus", 			F { return new FunctionPlus; });
	factory.registerFunction("minus", 			F { return new FunctionMinus; });
	factory.registerFunction("multiply", 		F { return new FunctionMultiply; });
	factory.registerFunction("divide", 			F { return new FunctionDivideFloating; });
	factory.registerFunction("intDiv", 			F { return new FunctionDivideIntegral; });
	factory.registerFunction("modulo", 			F { return new FunctionModulo; });
	factory.registerFunction("negate", 			F { return new FunctionNegate; });
	factory.registerFunction("bitAnd", 			F { return new FunctionBitAnd; });
	factory.registerFunction("bitOr", 			F { return new FunctionBitOr; });
	factory.registerFunction("bitXor", 			F { return new FunctionBitXor; });
	factory.registerFunction("bitNot", 			F { return new FunctionBitNot; });
	factory.registerFunction("bitShiftLeft", 	F { return new FunctionBitShiftLeft; });
	factory.registerFunction("bitShiftRight", 	F { return new FunctionBitShiftRight; });

	#undef F
}

}
