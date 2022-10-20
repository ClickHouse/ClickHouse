#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeSecondNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeSecondNumImpl<ResultPrecision::Standard>>;

void registerFunctionToRelativeSecondNum(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToRelativeSecondNum>();
}

}


