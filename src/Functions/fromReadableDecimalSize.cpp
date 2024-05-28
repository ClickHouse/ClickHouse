#include <base/types.h>
#include <Functions/FunctionFactory.h>
#include <Functions/fromReadable.h>

namespace DB
{

namespace
{

const std::unordered_map<std::string_view, size_t> scale_factors =
{
    {"b", 1L},
    {"kb", 1000L},
    {"mb", 1000L * 1000L},
    {"gb", 1000L * 1000L * 1000L},
    {"tb", 1000L * 1000L * 1000L * 1000L},
    {"pb", 1000L * 1000L * 1000L * 1000L * 1000L},
    {"eb", 1000L * 1000L * 1000L * 1000L * 1000L * 1000L},
};

struct Impl
{
    static const std::unordered_map<std::string_view, size_t> & getScaleFactors()
    {
        return scale_factors;
    }
};

struct NameFromReadableDecimalSize
{
    static constexpr auto name = "fromReadableDecimalSize";
};

struct NameFromReadableDecimalSizeOrNull
{
    static constexpr auto name = "fromReadableDecimalSizeOrNull";
};

struct NameFromReadableDecimalSizeOrZero
{
    static constexpr auto name = "fromReadableDecimalSizeOrZero";
};

using FunctionFromReadableDecimalSize = FunctionFromReadable<NameFromReadableDecimalSize, Impl, ErrorHandling::Exception>;
using FunctionFromReadableDecimalSizeOrNull = FunctionFromReadable<NameFromReadableDecimalSizeOrNull, Impl, ErrorHandling::Null>;
using FunctionFromReadableDecimalSizeOrZero = FunctionFromReadable<NameFromReadableDecimalSizeOrZero, Impl, ErrorHandling::Zero>;


FunctionDocumentation fromReadableDecimalSize_documentation {
    .description = "Given a string containing the readable representation of a byte size with decimal units this function returns the corresponding number of bytes.",
    .syntax = "fromReadableDecimalSize(x)",
    .arguments = {{"x", "Readable size with decimal units ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer ([UInt64](../../sql-reference/data-types/int-uint.md))",
    .examples = {
        {
            "basic",
            "SELECT arrayJoin(['1 B', '1 KB', '3 MB', '5.314 KB']) AS readable_sizes, fromReadableDecimalSize(readable_sizes) AS sizes;",
            R"(
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KB           │    1000 │
│ 3 MB           │ 3000000 │
│ 5.314 KB       │    5314 │
└────────────────┴─────────┘)"
        },
    },
    .categories = {"OtherFunctions"},
};

FunctionDocumentation fromReadableDecimalSizeOrNull_documentation {
    .description = "Given a string containing the readable representation of a byte size with decimal units this function returns the corresponding number of bytes, or `NULL` if unable to parse the value.",
    .syntax = "fromReadableDecimalSizeOrNull(x)",
    .arguments = {{"x", "Readable size with decimal units ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer, or NULL if unable to parse the input (Nullable([UInt64](../../sql-reference/data-types/int-uint.md)))",
    .examples = {
        {
            "basic",
            "SELECT arrayJoin(['1 B', '1 KB', '3 MB', '5.314 KB', 'invalid']) AS readable_sizes, fromReadableSizeOrNull(readable_sizes) AS sizes;",
            R"(
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KB           │    1000 │
│ 3 MB           │ 3000000 │
│ 5.314 KB       │    5314 │
│ invalid        │    ᴺᵁᴸᴸ │
└────────────────┴─────────┘)"
        },
    },
    .categories = {"OtherFunctions"},
};

FunctionDocumentation fromReadableDecimalSizeOrZero_documentation {
    .description = "Given a string containing the readable representation of a byte size with decimal units this function returns the corresponding number of bytes, or 0 if unable to parse the value.",
    .syntax = "formatReadableSizeOrZero(x)",
    .arguments = {{"x", "Readable size with decimal units ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer, or 0 if unable to parse the input ([UInt64](../../sql-reference/data-types/int-uint.md))",
    .examples = {
        {
            "basic",
            "SELECT arrayJoin(['1 B', '1 KB', '3 MB', '5.314 KB', 'invalid']) AS readable_sizes, fromReadableSizeOrZero(readable_sizes) AS sizes;",
            R"(
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KB           │    1000 │
│ 3 MB           │ 3000000 │
│ 5.314 KB       │    5000 │
│ invalid        │       0 │
└────────────────┴─────────┘)"
        },
    },
    .categories = {"OtherFunctions"},
};
}

REGISTER_FUNCTION(FromReadableDecimalSize)
{
    factory.registerFunction<FunctionFromReadableDecimalSize>(fromReadableDecimalSize_documentation);
    factory.registerFunction<FunctionFromReadableDecimalSizeOrNull>(fromReadableDecimalSizeOrNull_documentation);
    factory.registerFunction<FunctionFromReadableDecimalSizeOrZero>(fromReadableDecimalSizeOrZero_documentation);
}
}
