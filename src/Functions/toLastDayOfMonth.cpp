#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToLastDayOfMonth = FunctionDateOrDateTimeToSomething<DataTypeDate, ToLastDayOfMonthImpl>;

REGISTER_FUNCTION(ToLastDayOfMonth)
{
    factory.registerFunction<FunctionToLastDayOfMonth>();

    /// MySQL compatibility alias.
    factory.registerAlias("LAST_DAY", "toLastDayOfMonth", FunctionFactory::CaseInsensitive);
}

}
