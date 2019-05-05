#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToCustomWeek = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToCustomWeekImpl>;

void registerFunctionToCustomWeek(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToCustomWeek>();
}

}


