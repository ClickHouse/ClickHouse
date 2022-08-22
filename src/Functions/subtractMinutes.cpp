#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractMinutes = FunctionDateOrDateTimeAddInterval<SubtractMinutesImpl>;

void registerFunctionSubtractMinutes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractMinutes>();
}

}


