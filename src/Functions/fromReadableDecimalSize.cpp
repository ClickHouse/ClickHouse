#include <base/types.h>
#include <Functions/FunctionFactory.h>
#include <Functions/fromReadable.h>

namespace DB
{

namespace
{

struct Impl
{
    static const ScaleFactors & getScaleFactors()
    {
        static const ScaleFactors scale_factors =
        {
            {"b", 1ull},
            {"kb", 1000ull},
            {"mb", 1000ull * 1000ull},
            {"gb", 1000ull * 1000ull * 1000ull},
            {"tb", 1000ull * 1000ull * 1000ull * 1000ull},
            {"pb", 1000ull * 1000ull * 1000ull * 1000ull * 1000ull},
            {"eb", 1000ull * 1000ull * 1000ull * 1000ull * 1000ull * 1000ull},
        };

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
    .description = "Given a string containing a byte size and `B`, `KB`, `MB`, etc. as a unit, this function returns the corresponding number of bytes. If the function is unable to parse the input value, it throws an exception.",
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
    .description = "Given a string containing a byte size and `B`, `KiB`, `MiB`, etc. as a unit, this function returns the corresponding number of bytes. If the function is unable to parse the input value, it returns `NULL`",
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
    .description = "Given a string containing a byte size and `B`, `KiB`, `MiB`, etc. as a unit, this function returns the corresponding number of bytes. If the function is unable to parse the input value, it returns `0`",
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
