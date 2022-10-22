#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfFifteenMinutes = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfFifteenMinutesImpl>;

REGISTER_FUNCTION(ToStartOfFifteenMinutes)
{
    factory.registerFunction<FunctionToStartOfFifteenMinutes>();
}

}


