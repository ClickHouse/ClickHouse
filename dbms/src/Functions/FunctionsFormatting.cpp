#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsFormatting.h>

namespace DB
{

void registerFunctionsFormatting(FunctionFactory & factory)
{
	factory.registerFunction<FunctionBitmaskToList>();
	factory.registerFunction<FunctionFormatReadableSize>();
}

}
