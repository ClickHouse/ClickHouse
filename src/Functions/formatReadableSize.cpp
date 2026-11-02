#include <Functions/FunctionFactory.h>
#include <Functions/formatReadable.h>
#include <Common/formatReadable.h>


namespace DB
{

REGISTER_FUNCTION(FormatReadableSize)
{
    FunctionDocumentation::Description description = R"(
Given a size (number of bytes), this function returns a readable, rounded size with suffix (KiB, MiB, etc.) as string.

The opposite operations of this function are [`parseReadableSize`](#parseReadableSize), [`parseReadableSizeOrZero`](#parseReadableSizeOrZero), and [`parseReadableSizeOrNull`](#parseReadableSizeOrNull).
This function accepts any numeric type as input, but internally it casts them to `Float64`. Results might be suboptimal with large values.
    )";
    FunctionDocumentation::Syntax syntax = "formatReadableSize(value[, precision])";
    FunctionDocumentation::Arguments arguments = {
        {"value", "Size in bytes.", {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float32", "Float64", "Decimal"}},
        {"precision", "Optional. Number of digits after the decimal point. Defaults to 2.", {"const UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a readable, rounded size with suffix as a string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Format file sizes",
        R"(
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
        )",
        R"(
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
        )"
    },
    {
        "With explicit precision",
        R"(
SELECT
    formatReadableSize(192851925, 0) AS no_decimals,
    formatReadableSize(192851925, 4) AS four_decimals
        )",
        R"(
┌─no_decimals─┬─four_decimals──┐
│ 184 MiB     │ 183.9179 MiB   │
└─────────────┴────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("formatReadableSize", [](ContextPtr){ return FunctionFormatReadable::create("formatReadableSize", formatReadableSizeWithBinarySuffix); }, documentation);
    factory.registerAlias("FORMAT_BYTES", "formatReadableSize", FunctionFactory::Case::Insensitive);
}

}
