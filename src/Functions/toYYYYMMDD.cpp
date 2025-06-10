#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYYYYMMDD = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMDDImpl>;

REGISTER_FUNCTION(ToYYYYMMDD)
{
    FunctionDocumentation::Description description_toYYYYMMDD = R"(
Converts a date or date with time to a `UInt32` number containing the year and month number (YYYY * 10000 + MM * 100 + DD). Accepts a second optional timezone argument. If provided, the timezone must be a string constant.
    )";
    FunctionDocumentation::Syntax syntax_toYYYYMMDD = R"(
toYYYYMMDD(datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments_toYYYYMMDD = {
        {"datetime", "A date or date with time to convert. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."},
        {"timezone", "Optional. Timezone for the conversion. If provided, the timezone must be a string constant. [`String`](../data-types/string.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toYYYYMMDD = "Returns a `UInt32` number containing the year, month and day (YYYY * 10000 + MM * 100 + DD). [`UInt32`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toYYYYMMDD = {
        {"Convert current date to YYYYMMDD format", R"(
SELECT toYYYYMMDD(now(), 'US/Eastern')
        )",
        R"(
┌─toYYYYMMDD(now(), 'US/Eastern')─┐
│                        20230302 │
└─────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toYYYYMMDD = {1, 1};
    FunctionDocumentation::Category category_toYYYYMMDD = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toYYYYMMDD = {
        description_toYYYYMMDD,
        syntax_toYYYYMMDD,
        arguments_toYYYYMMDD,
        returned_value_toYYYYMMDD,
        examples_toYYYYMMDD,
        introduced_in_toYYYYMMDD,
        category_toYYYYMMDD
    };

    factory.registerFunction<FunctionToYYYYMMDD>(documentation_toYYYYMMDD);
}

}


