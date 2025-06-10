#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYYYYMMDDhhmmss = FunctionDateOrDateTimeToSomething<DataTypeUInt64, ToYYYYMMDDhhmmssImpl>;

REGISTER_FUNCTION(ToYYYYMMDDhhmmss)
{
    FunctionDocumentation::Description description_toYYYYMMDDhhmmss = R"(
Converts a date or date with time to a `UInt64` number containing the year and month number (YYYY * 10000000000 + MM * 100000000 + DD * 1000000 + hh * 10000 + mm * 100 + ss).
Accepts a second optional timezone argument. If provided, the timezone must be a string constant.
    )";
    FunctionDocumentation::Syntax syntax_toYYYYMMDDhhmmss = R"(
toYYYYMMDDhhmmss(datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments_toYYYYMMDDhhmmss = {
        {"datetime", "Date or date with time to convert. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."},
        {"timezone", "Optional. Timezone for the conversion. If provided, the timezone must be a string constant. [`String`](../data-types/string.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toYYYYMMDDhhmmss = "Returns a `UInt64` number containing the year, month, day, hour, minute and second (YYYY * 10000000000 + MM * 100000000 + DD * 1000000 + hh * 10000 + mm * 100 + ss). [`UInt64`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toYYYYMMDDhhmmss = {
        {"Convert current date and time to YYYYMMDDhhmmss format", R"(
SELECT toYYYYMMDDhhmmss(now(), 'US/Eastern')
        )",
        R"(
┌─toYYYYMMDDhhmmss(now(), 'US/Eastern')─┐
│                        20230302112209 │
└───────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toYYYYMMDDhhmmss = {1, 1};
    FunctionDocumentation::Category category_toYYYYMMDDhhmmss = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toYYYYMMDDhhmmss = {
        description_toYYYYMMDDhhmmss,
        syntax_toYYYYMMDDhhmmss,
        arguments_toYYYYMMDDhhmmss,
        returned_value_toYYYYMMDDhhmmss,
        examples_toYYYYMMDDhhmmss,
        introduced_in_toYYYYMMDDhhmmss,
        category_toYYYYMMDDhhmmss
    };

    factory.registerFunction<FunctionToYYYYMMDDhhmmss>(documentation_toYYYYMMDDhhmmss);
}

}


