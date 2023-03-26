#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToHour = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToHourImpl>;

REGISTER_FUNCTION(ToHour)
{
    factory.registerFunction<FunctionToHour>();

    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToHour>("HOUR", FunctionFactory::CaseInsensitive);
}

}


