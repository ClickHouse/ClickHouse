#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToISOYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToISOYearImpl>;

void registerFunctionToISOYear(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToISOYear>();
}

}


