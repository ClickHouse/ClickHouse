#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDayOfYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToDayOfYearImpl>;

void registerFunctionToDayOfYear(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToDayOfYear>();

    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToDayOfYear>("DAYOFYEAR", FunctionFactory::CaseInsensitive);
}

}


