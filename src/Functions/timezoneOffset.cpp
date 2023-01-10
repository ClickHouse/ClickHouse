#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctiontimezoneOffset = FunctionDateOrDateTimeToSomething<DataTypeInt32, TimezoneOffsetImpl>;

REGISTER_FUNCTION(timezoneOffset)
{
    factory.registerFunction<FunctiontimezoneOffset>();
    factory.registerAlias("timeZoneOffset", "timezoneOffset");
}

}


