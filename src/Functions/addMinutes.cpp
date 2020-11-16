#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMinutes = FunctionDateOrDateTimeAddInterval<AddMinutesImpl>;

void registerFunctionAddMinutes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddMinutes>();
}

}


