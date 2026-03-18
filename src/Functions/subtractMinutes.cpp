#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractMinutes = FunctionDateOrDateTimeAddInterval<SubtractMinutesImpl>;

REGISTER_FUNCTION(SubtractMinutes)
{
    factory.registerFunction<FunctionSubtractMinutes>();
}

}


