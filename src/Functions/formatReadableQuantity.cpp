#include <Functions/FunctionFactory.h>
#include <Functions/formatReadable.h>
#include <Common/formatReadable.h>


namespace DB
{

REGISTER_FUNCTION(FormatReadableQuantity)
{
    FunctionDocumentation::Description description = R"(
Given a number, this function returns a rounded number with suffix (thousand, million, billion, etc.) as a string.

This function accepts any numeric type as input, but internally it casts them to `Float64`.
Results might be suboptimal with large values.
    )";
    FunctionDocumentation::Syntax syntax = "formatReadableQuantity(value[, precision])";
    FunctionDocumentation::Arguments arguments = {
        {"value", "A number to format.", {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float32", "Float64", "Decimal"}},
        {"precision", "Optional. Number of digits after the decimal point. Defaults to 2.", {"const UInt8"}}
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
    },
    {
        "With explicit precision",
        R"(
SELECT
    formatReadableQuantity(98765432101234, 0) AS no_decimals,
    formatReadableQuantity(98765432101234, 4) AS four_decimals
        )",
        R"(
┌─no_decimals──┬─four_decimals─────┐
│ 99 trillion  │ 98.7654 trillion  │
└──────────────┴───────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("formatReadableQuantity", [](ContextPtr){ return FunctionFormatReadable::create("formatReadableQuantity", formatReadableQuantity); }, documentation);
}

}
