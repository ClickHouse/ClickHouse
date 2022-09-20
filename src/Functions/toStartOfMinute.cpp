#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfMinute = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfMinuteImpl>;

REGISTER_FUNCTION(ToStartOfMinute)
{
    factory.registerFunction<FunctionToStartOfMinute>();
}

}


