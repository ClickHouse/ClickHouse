#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsCoding.h>

namespace DB
{

void registerFunctionsCoding(FunctionFactory & factory)
{
	factory.registerFunction<FunctionToStringCutToZero>();
	factory.registerFunction<FunctionIPv6NumToString>();
	factory.registerFunction<FunctionIPv6StringToNum>();
	factory.registerFunction<FunctionIPv4NumToString>();
	factory.registerFunction<FunctionIPv4StringToNum>();
	factory.registerFunction<FunctionIPv4NumToStringClassC>();
	factory.registerFunction<FunctionHex>();
	factory.registerFunction<FunctionUnhex>();
	factory.registerFunction<FunctionBitmaskToArray>();
	factory.registerFunction<FunctionBitTest>();
	factory.registerFunction<FunctionBitTestAny>();
	factory.registerFunction<FunctionBitTestAll>();
}

}
