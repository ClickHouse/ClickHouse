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

    factory.registerFunction<FunctionToStartOfWeek>(FunctionDocumentation{.description=R"(
Rounds a date or date with time down to the nearest Sunday or Monday. Returns the date.
Syntax: toStartOfWeek(t[, mode[, timezone]])
The mode argument works exactly like the mode argument in function `toWeek()`. If no mode is specified, mode is assumed as 0.

Example:
[example:typical]
)",
    .examples{
        {"typical", "SELECT toStartOfWeek(today(), 1);", ""}},
    .categories{"Dates and Times"}}, FunctionFactory::CaseSensitive);

    factory.registerFunction<FunctionToLastDayOfWeek>(FunctionDocumentation{.description=R"(
Rounds a date or date with time up to the nearest Saturday or Sunday. Returns the date.
Syntax: toLastDayOfWeek(t[, mode[, timezone]])
The mode argument works exactly like the mode argument in function `toWeek()`. If no mode is specified, mode is assumed as 0.

Example:
[example:typical]
)",
    .examples{
        {"typical", "SELECT toLastDayOfWeek(today(), 1);", ""}},
    .categories{"Dates and Times"}}, FunctionFactory::CaseSensitive);

    /// Compatibility aliases for mysql.
    factory.registerAlias("week", "toWeek", FunctionFactory::CaseInsensitive);
    factory.registerAlias("yearweek", "toYearWeek", FunctionFactory::CaseInsensitive);
}

}
