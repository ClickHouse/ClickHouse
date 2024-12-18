#include <Common/FunctionDocumentation.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

using FunctionToMillisecond = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToMillisecondImpl>;

REGISTER_FUNCTION(ToMillisecond)
{
    factory.registerFunction<FunctionToMillisecond>(


        FunctionDocumentation{
            .description=R"(
Returns the millisecond component (0-999) of a date with time.
    )",
            .syntax="toMillisecond(value)",
            .arguments={{"value", "DateTime or DateTime64"}},
            .returned_value="The millisecond in the minute (0 - 59) of the given date/time",
            .examples{
                {"toMillisecond", "SELECT toMillisecond(toDateTime64('2023-04-21 10:20:30.456', 3)", "456"}},
            .categories{"Dates and Times"}
        }
            );

    /// MySQL compatibility alias.
    factory.registerAlias("MILLISECOND", "toMillisecond", FunctionFactory::Case::Insensitive);
}

}
