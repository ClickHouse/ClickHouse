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
    FunctionDocumentation::Description description = R"(
Given a size (number of bytes), this function returns a readable, rounded size with suffix (KB, MB, etc.) as a string.

The opposite operations of this function are [`parseReadableSize`](#parseReadableSize).
    )";
    FunctionDocumentation::Syntax syntax = "formatReadableDecimalSize(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Size in bytes.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a readable, rounded size with suffix as a string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Format file sizes", R"(
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableDecimalSize(filesize_bytes) AS filesize
        )",
        R"(
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.02 KB    │
│        1048576 │ 1.05 MB    │
│      192851925 │ 192.85 MB  │
└────────────────┴────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFormatReadable<Impl>>(documentation);
}

}
