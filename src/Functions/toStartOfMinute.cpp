#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfMinute = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfMinuteImpl>;

REGISTER_FUNCTION(ToStartOfMinute)
{
    factory.registerFunction<FunctionToStartOfMinute>();
}

}


