#include <Common/FunctionDocumentation.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

using FunctionToMillisecond = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToMillisecondImpl>;

REGISTER_FUNCTION(ToMillisecond)
{
    FunctionDocumentation::Description description_to_millisecond = R"(
Returns the millisecond component (0-999) of a `DateTime` or `DateTime64` value.
    )";
    FunctionDocumentation::Syntax syntax_to_millisecond = "toMillisecond(datetime)";
    FunctionDocumentation::Arguments arguments_to_millisecond =
    {
        {"datetime", "Date with time to get the millisecond from.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_millisecond = {"Returns the millisecond in the minute (0 - 59) of the given `Date` or `DateTime`", {"UInt16"}};
    FunctionDocumentation::Examples examples_to_millisecond = {
        {"Usage example", R"(
SELECT toMillisecond(toDateTime64('2023-04-21 10:20:30.456', 3));
        )",
        R"(
┌──toMillisecond(toDateTime64('2023-04-21 10:20:30.456', 3))─┐
│                                                        456 │
└────────────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_millisecond = {24, 2};
    FunctionDocumentation::Category category_to_millisecond = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_millisecond = {description_to_millisecond, syntax_to_millisecond, arguments_to_millisecond, returned_value_to_millisecond, examples_to_millisecond, introduced_in_to_millisecond, category_to_millisecond};

    factory.registerFunction<FunctionToMillisecond>(documentation_to_millisecond);

    /// MySQL compatibility alias.
    factory.registerAlias("MILLISECOND", "toMillisecond", FunctionFactory::Case::Insensitive);
}

}
