#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfDay = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfDayImpl>;

REGISTER_FUNCTION(ToStartOfDay)
{
    factory.registerFunction<FunctionToStartOfDay>();
}

}


