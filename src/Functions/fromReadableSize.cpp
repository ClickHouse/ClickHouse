#include <base/types.h>
#include <Functions/FunctionFactory.h>
#include <Functions/fromReadable.h>
#include "Common/FunctionDocumentation.h"

namespace DB
{

namespace
{

// ISO/IEC 80000-13 binary units
const std::unordered_map<std::string_view, size_t> scale_factors =
{
    {"b", 1L},
    {"kib", 1024L},
    {"mib", 1024L * 1024L},
    {"gib", 1024L * 1024L * 1024L},
    {"tib", 1024L * 1024L * 1024L * 1024L},
    {"pib", 1024L * 1024L * 1024L * 1024L * 1024L},
    {"eib", 1024L * 1024L * 1024L * 1024L * 1024L * 1024L},
};

struct Impl
{
    static const std::unordered_map<std::string_view, size_t> & getScaleFactors()
    {
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
    .description = "Given a string containing the readable representation of a byte size with ISO/IEC 80000-13 units this function returns the corresponding number of bytes.",
    .syntax = "fromReadableSize(x)",
    .arguments = {{"x", "Readable size with ISO/IEC 80000-13 units ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer ([UInt64](../../sql-reference/data-types/int-uint.md))",
    .examples = {
        {"example_integer", "SELECT fromReadableSize('1 KiB')", "1024"},
        {"example_decimal", "SELECT fromReadableSize('1.1 KiB')", "1127"},
    },
    .categories = {"OtherFunctions"},
};

FunctionDocumentation fromReadableSizeOrNull_documentation {
    .description = "Given a string containing the readable representation of a byte size with ISO/IEC 80000-13 units this function returns the corresponding number of bytes, or `NULL` if unable to parse the value.",
    .syntax = "fromReadableSizeOrNull(x)",
    .arguments = {{"x", "Readable size with ISO/IEC 80000-13 units ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer, or NULL if unable to parse the input (Nullable([UInt64](../../sql-reference/data-types/int-uint.md)))",
    .examples = {
        {"example_integer", "SELECT fromReadableSizeOrNull('1 KiB')", "1024"},
        {"example_decimal", "SELECT fromReadableSizeOrNull('1.1 KiB')", "1127"},
        {"example_null", "SELECT fromReadableSizeOrNull('invalid')", "NULL"},
    },
    .categories = {"OtherFunctions"},
};

FunctionDocumentation fromReadableSizeOrZero_documentation {
    .description = "Given a string containing the readable representation of a byte size with ISO/IEC 80000-13 units this function returns the corresponding number of bytes, or 0 if unable to parse the value.",
    .syntax = "fromReadableSizeOrZero(x)",
    .arguments = {{"x", "Readable size with ISO/IEC 80000-13 units ([String](../../sql-reference/data-types/string.md))"}},
    .returned_value = "Number of bytes, rounded up to the nearest integer, or 0 if unable to parse the input ([UInt64](../../sql-reference/data-types/int-uint.md))",
    .examples = {
        {"example_integer", "SELECT fromReadableSizeOrZero('1 KiB')", "1024"},
        {"example_decimal", "SELECT fromReadableSizeOrZero('1.1 KiB')", "1127"},
        {"example_null", "SELECT fromReadableSizeOrZero('invalid')", "0"},
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
