#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToLastDayOfMonth = FunctionDateOrDateTimeToSomething<DataTypeDate, ToLastDayOfMonthImpl>;

void registerFunctionToLastDayOfMonth(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToLastDayOfMonth>();

    /// MySQL compatibility alias.
    factory.registerFunction<FunctionToLastDayOfMonth>("LAST_DAY", FunctionFactory::CaseInsensitive);
}

}


