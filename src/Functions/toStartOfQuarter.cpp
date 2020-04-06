#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToStartOfQuarter = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfQuarterImpl>;

void registerFunctionToStartOfQuarter(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfQuarter>();
}

}


