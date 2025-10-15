#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfFifteenMinutes = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfFifteenMinutesImpl>;

REGISTER_FUNCTION(ToStartOfFifteenMinutes)
{
    factory.registerFunction<FunctionToStartOfFifteenMinutes>();
}

}


