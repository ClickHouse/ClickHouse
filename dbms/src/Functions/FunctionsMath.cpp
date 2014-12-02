#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsMath.h>

namespace DB
{

void registerFunctionsMath(FunctionFactory & factory)
{
	factory.registerFunction<FunctionE>();
	factory.registerFunction<FunctionPi>();
	factory.registerFunction<FunctionExp>();
	factory.registerFunction<FunctionLog>();
	factory.registerFunction<FunctionExp2>();
	factory.registerFunction<FunctionLog2>();
	factory.registerFunction<FunctionExp10>();
	factory.registerFunction<FunctionLog10>();
	factory.registerFunction<FunctionSqrt>();
	factory.registerFunction<FunctionCbrt>();
	factory.registerFunction<FunctionErf>();
	factory.registerFunction<FunctionErfc>();
	factory.registerFunction<FunctionLGamma>();
	factory.registerFunction<FunctionTGamma>();
	factory.registerFunction<FunctionSin>();
	factory.registerFunction<FunctionCos>();
	factory.registerFunction<FunctionTan>();
	factory.registerFunction<FunctionAsin>();
	factory.registerFunction<FunctionAcos>();
	factory.registerFunction<FunctionAtan>();
	factory.registerFunction<FunctionPow>();
}

}
