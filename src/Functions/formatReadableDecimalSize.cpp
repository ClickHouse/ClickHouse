#include <Functions/FunctionFactory.h>
#include <Functions/formatReadable.h>
#include <Common/formatReadable.h>


namespace DB
{

namespace
{
    struct Impl
    {
        static constexpr auto name = "formatReadableDecimalSize";

        static void format(double value, DB::WriteBuffer & out)
        {
            formatReadableSizeWithDecimalSuffix(value, out);
        }
    };
}

REGISTER_FUNCTION(FormatReadableDecimalSize)
{
    factory.registerFunction<FunctionFormatReadable<Impl>>(
    FunctionDocumentation{
        .description=R"(
Accepts the size (number of bytes). Returns a rounded size with a suffix (KB, MB, etc.) as a string.
)",
        .syntax="formatReadableDecimalSize(x)",
        .arguments={
            {"x", "Numeric value representing a number of bytes. (U)Int(8/16/32/64/128/256), Float(32/64)"}
        },
        .returned_value="Returns `X` with rounded size and a suffix. String."
        .examples{
            {"formatReadableDecimalSize", "SELECT formatReadableDecimalSize(1000)", ""}},
        .category=FunctionDocumentation::Category::Other
    });
}

}
