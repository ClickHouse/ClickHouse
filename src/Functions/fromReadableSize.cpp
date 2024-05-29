#include <base/types.h>
#include <Functions/FunctionFactory.h>
#include <Functions/fromReadable.h>
#include "Common/FunctionDocumentation.h"

namespace DB
{

namespace
{

struct Impl
{
    static const ScaleFactors & getScaleFactors()
    {
        // ISO/IEC 80000-13 binary units
        static const ScaleFactors scale_factors =
        {
            {"b", 1ull},
            {"kib", 1024ull},
            {"mib", 1024ull * 1024ull},
            {"gib", 1024ull * 1024ull * 1024ull},
            {"tib", 1024ull * 1024ull * 1024ull * 1024ull},
            {"pib", 1024ull * 1024ull * 1024ull * 1024ull * 1024ull},
            {"eib", 1024ull * 1024ull * 1024ull * 1024ull * 1024ull * 1024ull},
        };

        return scale_factors;
    }
};


struct NameFromReadableSize
{
    static constexpr auto name = "fromReadableSize";
};

struct NameFromReadableSizeOrNull
{
    static constexpr auto name = "fromReadableSizeOrNull";
};

struct NameFromReadableSizeOrZero
{
    static constexpr auto name = "fromReadableSizeOrZero";
};

using FunctionFromReadableSize = FunctionFromReadable<NameFromReadableSize, Impl, ErrorHandling::Exception>;
using FunctionFromReadableSizeOrNull = FunctionFromReadable<NameFromReadableSizeOrNull, Impl, ErrorHandling::Null>;
using FunctionFromReadableSizeOrZero = FunctionFromReadable<NameFromReadableSizeOrZero, Impl, ErrorHandling::Zero>;

FunctionDocumentation fromReadableSize_documentation {
    .description = "Given a string containing a byte size and `B`, `KiB`, `MiB`, etc. as a unit (i.e. [ISO/IEC 80000-13](https://en.wikipedia.org/wiki/ISO/IEC_80000) unit), this function returns the corresponding number of bytes. If the function is unable to parse the input value, it throws an exception.",
    .syntax = "fromReadableSize(x)",
    .arguments = {{"x", "Readable size with ISO/IEC 80000-13 units ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer ([UInt64](../../sql-reference/data-types/int-uint.md))",
    .examples = {
        {
            "basic",
            "SELECT arrayJoin(['1 B', '1 KiB', '3 MiB', '5.314 KiB']) AS readable_sizes, fromReadableSize(readable_sizes) AS sizes;",
            R"(
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KiB          │    1024 │
│ 3 MiB          │ 3145728 │
│ 5.314 KiB      │    5442 │
└────────────────┴─────────┘)"
        },
    },
    .categories = {"OtherFunctions"},
};

FunctionDocumentation fromReadableSizeOrNull_documentation {
    .description = "Given a string containing a byte size and `B`, `KiB`, `MiB`, etc. as a unit (i.e. [ISO/IEC 80000-13](https://en.wikipedia.org/wiki/ISO/IEC_80000) unit), this function returns the corresponding number of bytes. If the function is unable to parse the input value, it returns `NULL`",
    .syntax = "fromReadableSizeOrNull(x)",
    .arguments = {{"x", "Readable size with ISO/IEC 80000-13 units ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer, or NULL if unable to parse the input (Nullable([UInt64](../../sql-reference/data-types/int-uint.md)))",
    .examples = {
        {
            "basic",
            "SELECT arrayJoin(['1 B', '1 KiB', '3 MiB', '5.314 KiB', 'invalid']) AS readable_sizes, fromReadableSize(readable_sizes) AS sizes;",
            R"(
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KiB          │    1024 │
│ 3 MiB          │ 3145728 │
│ 5.314 KiB      │    5442 │
│ invalid        │    ᴺᵁᴸᴸ │
└────────────────┴─────────┘)"
        },
    },
    .categories = {"OtherFunctions"},
};

FunctionDocumentation fromReadableSizeOrZero_documentation {
    .description = "Given a string containing a byte size and `B`, `KiB`, `MiB`, etc. as a unit (i.e. [ISO/IEC 80000-13](https://en.wikipedia.org/wiki/ISO/IEC_80000) unit), this function returns the corresponding number of bytes. If the function is unable to parse the input value, it returns `0`",
    .syntax = "fromReadableSizeOrZero(x)",
    .arguments = {{"x", "Readable size with ISO/IEC 80000-13 units ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer, or 0 if unable to parse the input ([UInt64](../../sql-reference/data-types/int-uint.md))",
    .examples = {
        {
            "basic",
            "SELECT arrayJoin(['1 B', '1 KiB', '3 MiB', '5.314 KiB', 'invalid']) AS readable_sizes, fromReadableSize(readable_sizes) AS sizes;",
            R"(
┌─readable_sizes─┬───sizes─┐
│ 1 B            │       1 │
│ 1 KiB          │    1024 │
│ 3 MiB          │ 3145728 │
│ 5.314 KiB      │    5442 │
│ invalid        │       0 │
└────────────────┴─────────┘)",
        },
    },
    .categories = {"OtherFunctions"},
};
}

REGISTER_FUNCTION(FromReadableSize)
{
    factory.registerFunction<FunctionFromReadableSize>(fromReadableSize_documentation);
    factory.registerFunction<FunctionFromReadableSizeOrNull>(fromReadableSizeOrNull_documentation);
    factory.registerFunction<FunctionFromReadableSizeOrZero>(fromReadableSizeOrZero_documentation);
}
}
