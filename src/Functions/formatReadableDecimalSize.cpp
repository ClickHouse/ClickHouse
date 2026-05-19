#include <Functions/FunctionFactory.h>
#include <Functions/formatReadable.h>
#include <Common/formatReadable.h>


namespace DB
{

REGISTER_FUNCTION(FormatReadableDecimalSize)
{
    FunctionDocumentation::Description description = R"(
Given a size (number of bytes), this function returns a readable, rounded size with suffix (KB, MB, etc.) as a string.

The opposite operations of this function are [`parseReadableSize`](#parseReadableSize).
    )";
    FunctionDocumentation::Syntax syntax = "formatReadableDecimalSize(x[, precision])";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Size in bytes.", {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float32", "Float64", "Decimal"}},
        {"precision", "Optional. Number of digits after the decimal point. Defaults to 2.", {"const UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a readable, rounded size with suffix as a string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Format file sizes", R"(
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableDecimalSize(filesize_bytes) AS filesize
        )",
        R"(
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.02 KB    │
│        1048576 │ 1.05 MB    │
│      192851925 │ 192.85 MB  │
└────────────────┴────────────┘
        )"
    },
    {
        "With explicit precision",
        R"(
SELECT
    formatReadableDecimalSize(192851925, 0) AS no_decimals,
    formatReadableDecimalSize(192851925, 4) AS four_decimals
        )",
        R"(
┌─no_decimals─┬─four_decimals─┐
│ 193 MB      │ 192.8519 MB   │
└─────────────┴───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("formatReadableDecimalSize", [](ContextPtr){ return FunctionFormatReadable::create("formatReadableDecimalSize", formatReadableSizeWithDecimalSuffix); }, documentation);
}

}
