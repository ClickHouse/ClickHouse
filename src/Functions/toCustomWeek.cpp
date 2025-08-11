#include <DataTypes/DataTypesNumber.h>
#include <Functions/CustomWeekTransforms.h>
#include <Functions/FunctionCustomWeekToSomething.h>
#include <Functions/FunctionCustomWeekToDateOrDate32.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>


namespace DB
{
using FunctionToWeek = FunctionCustomWeekToSomething<DataTypeUInt8, ToWeekImpl>;
using FunctionToYearWeek = FunctionCustomWeekToSomething<DataTypeUInt32, ToYearWeekImpl>;
using FunctionToStartOfWeek = FunctionCustomWeekToDateOrDate32<ToStartOfWeekImpl>;
using FunctionToLastDayOfWeek = FunctionCustomWeekToDateOrDate32<ToLastDayOfWeekImpl>;

REGISTER_FUNCTION(ToCustomWeek)
{
    factory.registerFunction<FunctionToWeek>();
    factory.registerFunction<FunctionToYearWeek>();
    factory.registerFunction<FunctionToStartOfWeek>();
    factory.registerFunction<FunctionToLastDayOfWeek>();

    /// Compatibility aliases for mysql.
    factory.registerAlias("week", "toWeek", FunctionFactory::Case::Insensitive);
    factory.registerAlias("yearweek", "toYearWeek", FunctionFactory::Case::Insensitive);
}

}
