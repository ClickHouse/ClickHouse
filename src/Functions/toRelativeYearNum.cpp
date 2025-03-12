#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeYearNum = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToRelativeYearNumImpl<ResultPrecision::Standard>>;
using FunctionToYearNumSinceEpoch = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToYearNumSinceEpochImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeYearNum)
{
    factory.registerFunction<FunctionToRelativeYearNum>();
    factory.registerFunction<FunctionToYearNumSinceEpoch>();
}

}


