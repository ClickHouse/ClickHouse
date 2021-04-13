#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToQuarter = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToQuarterImpl>;

void registerFunctionToQuarter(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToQuarter>();
}

}


