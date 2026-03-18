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

    /// MySQL compatibility alias.
    factory.registerAlias("DAY", "toDayOfMonth", FunctionFactory::Case::Insensitive);
    factory.registerAlias("DAYOFMONTH", "toDayOfMonth", FunctionFactory::Case::Insensitive);
}

}
