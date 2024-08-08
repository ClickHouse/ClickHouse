#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDayOfYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToDayOfYearImpl>;

REGISTER_FUNCTION(ToDayOfYear)
{
    factory.registerFunction<FunctionToDayOfYear>();

    /// MySQL compatibility alias.
    factory.registerAlias("DAYOFYEAR", "toDayOfYear", FunctionFactory::Case::Insensitive);
}

}
