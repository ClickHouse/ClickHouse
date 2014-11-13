#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsComparison.h>

namespace DB
{

void registerFunctionsComparison(FunctionFactory & factory)
{
	factory.registerFunction<FunctionEquals>();
	factory.registerFunction<FunctionNotEquals>();
	factory.registerFunction<FunctionLess>();
	factory.registerFunction<FunctionGreater>();
	factory.registerFunction<FunctionLessOrEquals>();
	factory.registerFunction<FunctionGreaterOrEquals>();
}

}
