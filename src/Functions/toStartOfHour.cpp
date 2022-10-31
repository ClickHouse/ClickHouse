#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfHour = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfHourImpl>;

REGISTER_FUNCTION(ToStartOfHour)
{
    factory.registerFunction<FunctionToStartOfHour>();
}

}


