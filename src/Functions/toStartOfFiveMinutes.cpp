#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfFiveMinutes = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfFiveMinutesImpl>;

REGISTER_FUNCTION(ToStartOfFiveMinutes)
{
    factory.registerFunction<FunctionToStartOfFiveMinutes>();
    factory.registerAlias("toStartOfFiveMinute", FunctionToStartOfFiveMinutes::name);
}

}


