#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDayOfMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfMonthImpl>;

REGISTER_FUNCTION(ToDayOfMonth)
{
    factory.registerFunction<FunctionToDayOfMonth>();

    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToDayOfMonth>("DAY", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionToDayOfMonth>("DAYOFMONTH", FunctionFactory::CaseInsensitive);
}

}


