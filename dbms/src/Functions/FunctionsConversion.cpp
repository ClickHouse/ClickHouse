#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsConversion.h>

namespace DB
{

void registerFunctionsConversion(FunctionFactory & factory)
{
	factory.registerFunction<FunctionToUInt8>();
	factory.registerFunction<FunctionToUInt16>();
	factory.registerFunction<FunctionToUInt32>();
	factory.registerFunction<FunctionToUInt64>();
	factory.registerFunction<FunctionToInt8>();
	factory.registerFunction<FunctionToInt16>();
	factory.registerFunction<FunctionToInt32>();
	factory.registerFunction<FunctionToInt64>();
	factory.registerFunction<FunctionToFloat32>();
	factory.registerFunction<FunctionToFloat64>();
	factory.registerFunction<FunctionToDate>();
	factory.registerFunction<FunctionToDateTime>();
	factory.registerFunction<FunctionToString>();
	factory.registerFunction<FunctionToFixedString>();
	factory.registerFunction<FunctionToUnixTimestamp>();
}

}
