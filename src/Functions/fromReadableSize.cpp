#include <base/types.h>
#include <Functions/FunctionFactory.h>
#include <Functions/fromReadable.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
}

namespace
{

// ISO/IEC 80000-13 binary units
const std::unordered_map<std::string_view, Float64> scale_factors =
{
    {"b", 1.0},
    {"kib", 1024.0},
    {"mib", 1024.0 * 1024.0},
    {"gib", 1024.0 * 1024.0 * 1024.0},
    {"tib", 1024.0 * 1024.0 * 1024.0 * 1024.0},
    {"pib", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0},
    {"eib", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0},
};

struct Impl
{
    static constexpr auto name = "fromReadableSize";

    static Float64 getScaleFactorForUnit(const String & unit)  // Assumes the unit is already in lowercase
    {
        auto iter = scale_factors.find(unit);
        if (iter == scale_factors.end())
        {
            throw Exception(
                ErrorCodes::CANNOT_PARSE_TEXT,
                "Invalid expression for function {} - Unknown readable size unit (\"{}\")",
                name,
                unit
            );
        }
        return iter->second;
    }

};
}

REGISTER_FUNCTION(FromReadableSize)
{
    factory.registerFunction<FunctionFromReadable<Impl>>(FunctionDocumentation
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
}

}
