#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToMillisecond = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMillisecondImpl>;

REGISTER_FUNCTION(ToMillisecond)
{
    factory.registerFunction<FunctionToMillisecond>();

    /// MySQL compatibility alias.
    factory.registerAlias("MILLISECOND", "toMllisecond", FunctionFactory::CaseInsensitive);
}

}