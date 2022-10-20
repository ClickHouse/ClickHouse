#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeYearNum = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToRelativeYearNumImpl<ResultPrecision::Standard>>;

void registerFunctionToRelativeYearNum(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToRelativeYearNum>();
}

}


