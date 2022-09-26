#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMonthImpl>;

REGISTER_FUNCTION(ToMonth)
{
    factory.registerFunction<FunctionToMonth>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToMonth>("MONTH", FunctionFactory::CaseInsensitive);
}

}


