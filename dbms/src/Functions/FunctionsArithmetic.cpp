#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionsArithmetic(FunctionFactory & factory)
{
	factory.registerFunction<FunctionPlus>();
	factory.registerFunction<FunctionMinus>();
	factory.registerFunction<FunctionMultiply>();
	factory.registerFunction<FunctionDivideFloating>();
	factory.registerFunction<FunctionDivideIntegral>();
	factory.registerFunction<FunctionDivideIntegralOrZero>();
	factory.registerFunction<FunctionModulo>();
	factory.registerFunction<FunctionNegate>();
	factory.registerFunction<FunctionAbs>();
	factory.registerFunction<FunctionBitAnd>();
	factory.registerFunction<FunctionBitOr>();
	factory.registerFunction<FunctionBitXor>();
	factory.registerFunction<FunctionBitNot>();
	factory.registerFunction<FunctionBitShiftLeft>();
	factory.registerFunction<FunctionBitShiftRight>();
}

}
