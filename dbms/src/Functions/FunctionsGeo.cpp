#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsGeo.h>


namespace DB
{

void registerFunctionsGeo(FunctionFactory & factory)
{
	factory.registerFunction<FunctionGreatCircleDistance>();
}
}
