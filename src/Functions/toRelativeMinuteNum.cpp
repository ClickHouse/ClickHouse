#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeMinuteNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeMinuteNumImpl>;

void registerFunctionToRelativeMinuteNum(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToRelativeMinuteNum>();
}

}


