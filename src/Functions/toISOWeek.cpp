#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToISOWeek = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToISOWeekImpl>;

REGISTER_FUNCTION(ToISOWeek)
{
    factory.registerFunction<FunctionToISOWeek>();
}

}


