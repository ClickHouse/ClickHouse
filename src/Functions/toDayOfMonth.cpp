#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDayOfMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfMonthImpl>;

void registerFunctionToDayOfMonth(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToDayOfMonth>();
}

}


