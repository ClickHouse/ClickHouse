#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYYYYMM = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMImpl>;

REGISTER_FUNCTION(ToYYYYMM)
{
    FunctionDocumentation::Description description_toYYYYMM = R"(
Converts a date or date with time to a `UInt32` number containing the year and month number (YYYY * 100 + MM).
Accepts a second optional timezone argument. If provided, the timezone must be a string constant.

This function is the opposite of function `YYYYMMDDToDate()`.
    )";
    FunctionDocumentation::Syntax syntax_toYYYYMM = R"(
toYYYYMM(datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments_toYYYYMM = {
        {"datetime", "A date or date with time to convert. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."},
        {"timezone", "Optional. Timezone for the conversion. If provided, the timezone must be a string constant. [`String`](../data-types/string.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toYYYYMM = "Returns a UInt32 number containing the year and month number (YYYY * 100 + MM). [`UInt32`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toYYYYMM = {
        {"Convert current date to YYYYMM format", R"(
SELECT toYYYYMM(now(), 'US/Eastern')
        )",
        R"(
┌─toYYYYMM(now(), 'US/Eastern')─┐
│                        202303 │
└───────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toYYYYMM = {1, 1};
    FunctionDocumentation::Category category_toYYYYMM = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toYYYYMM = {
        description_toYYYYMM,
        syntax_toYYYYMM,
        arguments_toYYYYMM,
        returned_value_toYYYYMM,
        examples_toYYYYMM,
        introduced_in_toYYYYMM,
        category_toYYYYMM
    };

    factory.registerFunction<FunctionToYYYYMM>(documentation_toYYYYMM);
}

}


