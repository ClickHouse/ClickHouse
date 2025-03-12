#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeMonthNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeMonthNumImpl<ResultPrecision::Standard>>;
using FunctionToMonthNumSinceEpoch = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToMonthNumSinceEpochImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeMonthNum)
{
    factory.registerFunction<FunctionToRelativeMonthNum>();
    factory.registerFunction<FunctionToMonthNumSinceEpoch>();
}

}


