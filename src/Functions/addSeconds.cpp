#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddSeconds = FunctionDateOrDateTimeAddInterval<AddSecondsImpl>;

void registerFunctionAddSeconds(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddSeconds>();
}

}


