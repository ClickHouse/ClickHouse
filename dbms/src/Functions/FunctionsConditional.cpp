#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsConditional.h>

namespace DB
{

void registerFunctionsConditional(FunctionFactory & factory)
{
	factory.registerFunction<FunctionIf>();
	factory.registerFunction<FunctionMultiIf>();
}

}
