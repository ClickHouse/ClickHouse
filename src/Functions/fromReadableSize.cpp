#include <base/types.h>
#include <Functions/FunctionFactory.h>
#include <Functions/fromReadable.h>

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

}

REGISTER_FUNCTION(FromReadableSize)
{
    factory.registerFunction<FunctionFromReadableSize>(FunctionDocumentation
        {
            .description=R"(
Given a string containing the readable representation of a byte size, this function returns the corresponding number of bytes:
[example:basic_binary]
[example:basic_decimal]

Accepts readable sizes up to the Exabyte (EB/EiB).

)",
            .examples{
                {"basic_binary", "SELECT fromReadableSize('1 KiB')", "1024"},
                {"basic_decimal", "SELECT fromReadableSize('1.523 KB')", "1523"},
            },
            .categories{"OtherFunctions"}
        }
    );
    factory.registerFunction<FunctionFromReadableSizeOrNull>();
    factory.registerFunction<FunctionFromReadableSizeOrZero>();
}
}
