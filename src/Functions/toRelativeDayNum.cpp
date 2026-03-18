#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeDayNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeDayNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeDayNum)
{
    factory.registerFunction<FunctionToRelativeDayNum>();
}

}


