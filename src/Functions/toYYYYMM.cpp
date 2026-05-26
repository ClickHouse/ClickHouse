#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYYYYMM = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMImpl>;

REGISTER_FUNCTION(ToYYYYMM)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to a `UInt32` number containing the year and month number (YYYY * 100 + MM).
Accepts a second optional timezone argument. If provided, the timezone must be a string constant.

This function is the opposite of function `YYYYMMDDToDate()`.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toYYYYMM(datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date or date with time to convert.", {"Date", "Date32", "DateTime", "DateTime64"}},
        {"timezone", "Optional. Timezone for the conversion. If provided, the timezone must be a string constant.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a UInt32 number containing the year and month number (YYYY * 100 + MM).", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Convert current date to YYYYMM format", R"(
SELECT toYYYYMM(now(), 'US/Eastern')
        )",
        R"(
┌─toYYYYMM(now(), 'US/Eastern')─┐
│                        202303 │
└───────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToYYYYMM>(documentation);
}

}


