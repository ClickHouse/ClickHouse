#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsGeo.h>


namespace DB
{

void registerFunctionsGeo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreatCircleDistance>();
    factory.registerFunction<FunctionPointInEllipses>();
}
}
