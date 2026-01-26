#include <Functions/FunctionFactory.h>
#include <Functions/formatReadable.h>
#include <Common/formatReadable.h>


namespace DB
{

namespace
{
    struct Impl
    {
        static constexpr auto name = "formatReadableQuantity";

        static void format(double value, DB::WriteBuffer & out)
        {
            formatReadableQuantity(value, out);
        }
    };
}

REGISTER_FUNCTION(FormatReadableQuantity)
{
    FunctionDocumentation::Description description = R"(
Given a number, this function returns a rounded number with suffix (thousand, million, billion, etc.) as a string.

This function accepts any numeric type as input, but internally it casts them to `Float64`.
Results might be suboptimal with large values.
    )";
    FunctionDocumentation::Syntax syntax = "formatReadableQuantity(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "A number to format.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a rounded number with suffix as a string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Format numbers with suffixes",
        R"(
SELECT
    arrayJoin([1024, 1234 * 1000, (4567 * 1000) * 1000, 98765432101234]) AS number,
    formatReadableQuantity(number) AS number_for_humans
        )",
        R"(
┌─────────number─┬─number_for_humans─┐
│           1024 │ 1.02 thousand     │
│        1234000 │ 1.23 million      │
│     4567000000 │ 4.57 billion      │
│ 98765432101234 │ 98.77 trillion    │
└────────────────┴───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFormatReadable<Impl>>(documentation);
}

}
