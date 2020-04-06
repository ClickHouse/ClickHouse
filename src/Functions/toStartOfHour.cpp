#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToStartOfHour = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfHourImpl>;

void registerFunctionToStartOfHour(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfHour>();
}

}


