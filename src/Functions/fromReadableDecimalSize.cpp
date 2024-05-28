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

}

REGISTER_FUNCTION(FromReadableDecimalSize)
{
    factory.registerFunction<FunctionFromReadableDecimalSize>(FunctionDocumentation
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
    factory.registerFunction<FunctionFromReadableDecimalSizeOrNull>();
    factory.registerFunction<FunctionFromReadableDecimalSizeOrZero>();
}

}
