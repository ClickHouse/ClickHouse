#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsHigherOrder.h>

namespace DB
{

void registerFunctionsHigherOrder(FunctionFactory & factory)
{
	factory.registerFunction<FunctionArrayMap>();
	factory.registerFunction<FunctionArrayFilter>();
	factory.registerFunction<FunctionArrayCount>();
	factory.registerFunction<FunctionArrayExists>();
	factory.registerFunction<FunctionArrayAll>();
	factory.registerFunction<FunctionArraySum>();
	factory.registerFunction<FunctionArrayFirst>();
	factory.registerFunction<FunctionArrayFirstIndex>();
}

}
