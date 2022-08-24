#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToYearImpl>;

REGISTER_FUNCTION(ToYear)
{
    factory.registerFunction<FunctionToYear>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToYear>("YEAR", FunctionFactory::CaseInsensitive);
}

}


