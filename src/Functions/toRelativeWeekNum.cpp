#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeWeekNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeWeekNumImpl<false>>;

void registerFunctionToRelativeWeekNum(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToRelativeWeekNum>();
}

}


