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
        .examples{
            {"formatReadableDecimalSize", "SELECT formatReadableDecimalSize(1000)", ""}},
        .categories{"OtherFunctions"}
    });
}

}
