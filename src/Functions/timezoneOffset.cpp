#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctiontimezoneOffset = FunctionDateOrDateTimeToSomething<DataTypeInt32, TimezoneOffsetImpl>;

void registerFunctiontimezoneOffset(FunctionFactory & factory)
{
    factory.registerFunction<FunctiontimezoneOffset>();
    factory.registerAlias("timeZoneOffset", "timezoneOffset");
}

}


