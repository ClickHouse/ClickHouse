#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYYYYMMDDhhmmss = FunctionDateOrDateTimeToSomething<DataTypeUInt64, ToYYYYMMDDhhmmssImpl>;

REGISTER_FUNCTION(ToYYYYMMDDhhmmss)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to a `UInt64` number containing the year and month number (YYYY * 10000000000 + MM * 100000000 + DD * 1000000 + hh * 10000 + mm * 100 + ss).
Accepts a second optional timezone argument. If provided, the timezone must be a string constant.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toYYYYMMDDhhmmss(datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date or date with time to convert.", {"Date", "Date32", "DateTime", "DateTime64"}},
        {"timezone", "Optional. Timezone for the conversion. If provided, the timezone must be a string constant.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a `UInt64` number containing the year, month, day, hour, minute and second (YYYY * 10000000000 + MM * 100000000 + DD * 1000000 + hh * 10000 + mm * 100 + ss).", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
        {"Convert current date and time to YYYYMMDDhhmmss format", R"(
SELECT toYYYYMMDDhhmmss(now(), 'US/Eastern')
        )",
        R"(
┌─toYYYYMMDDhhmmss(now(), 'US/Eastern')─┐
│                        20230302112209 │
└───────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToYYYYMMDDhhmmss>(documentation);
}

}


