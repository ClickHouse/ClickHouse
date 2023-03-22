#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeHourNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeHourNumImpl>;

void registerFunctionToRelativeHourNum(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToRelativeHourNum>();
}

}


