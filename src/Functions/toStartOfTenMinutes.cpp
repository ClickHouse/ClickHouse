#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfTenMinutes = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfTenMinutesImpl>;

REGISTER_FUNCTION(ToStartOfTenMinutes)
{
    factory.registerFunction<FunctionToStartOfTenMinutes>();
}

}


