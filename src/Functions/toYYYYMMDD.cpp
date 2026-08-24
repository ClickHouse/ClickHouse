#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYYYYMMDD = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMDDImpl>;

REGISTER_FUNCTION(ToYYYYMMDD)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to a `UInt32` number containing the year and month number (YYYY * 10000 + MM * 100 + DD). Accepts a second optional timezone argument. If provided, the timezone must be a string constant.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toYYYYMMDD(datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date or date with time to convert.", {"Date", "Date32", "DateTime", "DateTime64"}},
        {"timezone", "Optional. Timezone for the conversion. If provided, the timezone must be a string constant.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a `UInt32` number containing the year, month and day (YYYY * 10000 + MM * 100 + DD).", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Convert current date to YYYYMMDD format", R"(
SELECT toYYYYMMDD(now(), 'US/Eastern')
        )",
        R"(
┌─toYYYYMMDD(now(), 'US/Eastern')─┐
│                        20230302 │
└─────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToYYYYMMDD>(documentation);
}

}


