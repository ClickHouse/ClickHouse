#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToISOWeek = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToISOWeekImpl>;

void registerFunctionToISOWeek(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToISOWeek>();
}

}


