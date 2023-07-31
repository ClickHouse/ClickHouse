#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeHourNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeHourNumImpl>;

REGISTER_FUNCTION(ToRelativeHourNum)
{
    factory.registerFunction<FunctionToRelativeHourNum>();
}

}


