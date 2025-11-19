#include <Functions/FunctionFactory.h>
#include <Functions/formatReadable.h>
#include <Common/formatReadable.h>


namespace DB
{

namespace
{
    struct Impl
    {
        static constexpr auto name = "formatReadableSize";

        static void format(double value, DB::WriteBuffer & out)
        {
            formatReadableSizeWithBinarySuffix(value, out);
        }
    };
}

REGISTER_FUNCTION(FormatReadableSize)
{
    FunctionDocumentation::Description description = R"(
Given a size (number of bytes), this function returns a readable, rounded size with suffix (KiB, MiB, etc.) as string.

The opposite operations of this function are [`parseReadableSize`](#parseReadableSize), [`parseReadableSizeOrZero`](#parseReadableSizeOrZero), and [`parseReadableSizeOrNull`](#parseReadableSizeOrNull).
This function accepts any numeric type as input, but internally it casts them to `Float64`. Results might be suboptimal with large values.
    )";
    FunctionDocumentation::Syntax syntax = "formatReadableSize(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Size in bytes.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a readable, rounded size with suffix as a string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Format file sizes",
        R"(
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
        )",
        R"(
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFormatReadable<Impl>>(documentation);
    factory.registerAlias("FORMAT_BYTES", Impl::name, FunctionFactory::Case::Insensitive);
}

}
