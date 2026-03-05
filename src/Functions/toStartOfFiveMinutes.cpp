#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfFiveMinutes = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfFiveMinutesImpl>;

REGISTER_FUNCTION(ToStartOfFiveMinutes)
{
    factory.registerFunction<FunctionToStartOfFiveMinutes>();
    factory.registerAlias("toStartOfFiveMinute", FunctionToStartOfFiveMinutes::name);
}

}


