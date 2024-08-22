#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToQuarter = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToQuarterImpl>;

REGISTER_FUNCTION(ToQuarter)
{
    factory.registerFunction<FunctionToQuarter>();
    /// MySQL compatibility alias.
    factory.registerAlias("QUARTER", "toQuarter", FunctionFactory::Case::Insensitive);
}

}
