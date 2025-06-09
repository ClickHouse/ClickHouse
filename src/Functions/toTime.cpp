#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToTime = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToTimeImpl>;

REGISTER_FUNCTION(ToTime)
{
    FunctionDocumentation::Description description_toTime = R"(
Converts a date with time to a certain fixed date, while preserving the time.

If the `date` input argument contained sub-second components, they will be dropped in the returned `DateTime` value with second-accuracy.
    )";
    FunctionDocumentation::Syntax syntax_toTime = R"(
toTime(date[, timezone])
    )";
    FunctionDocumentation::Arguments arguments_toTime = {
        {"date", "Date to convert to a time. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."},
        {"timezone", "Optional. Timezone for the returned value. [`String`](../data-types/string.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toTime = "Returns DateTime with date equated to `1970-01-02` while preserving the time. [`DateTime`](../data-types/datetime.md).";
    FunctionDocumentation::Examples examples_toTime = {
        {"Convert DateTime64 to time", R"(
SELECT toTime(toDateTime64('1970-12-10 01:20:30.3000',3)) AS result, toTypeName(result)
        )",
        R"(
┌──────────────result─┬─toTypeName(result)─┐
│ 1970-01-02 01:20:30 │ DateTime           │
└─────────────────────┴────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toTime = {1, 1};
    FunctionDocumentation::Category category_toTime = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toTime = {
        description_toTime,
        syntax_toTime,
        arguments_toTime,
        returned_value_toTime,
        examples_toTime,
        introduced_in_toTime,
        category_toTime
    };

    factory.registerFunction<FunctionToTime>(documentation_toTime);
}

}


