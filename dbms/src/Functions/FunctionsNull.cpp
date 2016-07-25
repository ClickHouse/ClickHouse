#include <DB/Functions/FunctionsNull.h>
#include <DB/Functions/FunctionFactory.h>

namespace DB
{

void registerFunctionsNull(FunctionFactory & factory)
{
	factory.registerFunction<FunctionIsNull>();
	factory.registerFunction<FunctionIsNotNull>();
	factory.registerFunction<FunctionCoalesce>();
}

}
