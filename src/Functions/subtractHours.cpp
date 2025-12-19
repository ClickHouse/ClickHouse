#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractHours = FunctionDateOrDateTimeAddInterval<SubtractHoursImpl>;

REGISTER_FUNCTION(SubtractHours)
{
    factory.registerFunction<FunctionSubtractHours>();
}

}


