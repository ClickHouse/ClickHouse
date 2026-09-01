#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToTimeWithFixedDate = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToTimeWithFixedDateImpl>;

REGISTER_FUNCTION(ToTimeWithFixedDate)
{
    FunctionDocumentation::Description description = R"(
Extracts the time component of a date or date with time.
The returned result is an offset to a fixed point in time, currently `1970-01-02`,
but the exact point in time is an implementation detail which may change in future.

`toTime` should therefore not be used standalone.
The main purpose of the function is to calculate the time difference between two dates or dates with time, e.g., `toTime(dt1) - toTime(dt2)`.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toTimeWithFixedDate(date[, timezone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"date", "Date to convert to a time.", {"Date", "DateTime", "DateTime64"}},
        {"timezone", "Optional. Timezone for the returned value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the time component of a date or date with time in the form of an offset to a fixed point in time (selected as 1970-01-02, currently).", {"DateTime"}};
    FunctionDocumentation::Examples examples = {
        {"Calculate the time difference between two dates", R"(
SELECT toTimeWithFixedDate('2025-06-15 12:00:00'::DateTime) - toTimeWithFixedDate('2024-05-10 11:00:00'::DateTime) AS result, toTypeName(result)
        )",
        R"(
┌─result─┬─toTypeName(result)─┐
│   3600 │ Int32              │
└────────┴────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToTimeWithFixedDate>(documentation);
}

}


