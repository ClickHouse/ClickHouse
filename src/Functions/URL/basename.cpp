#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/StringHelpers.h>


namespace DB
{

/** Extract substring after the last slash or backslash.
  * If there are no slashes, return the string unchanged.
  * It is used to extract filename from path.
  */
struct ExtractBasename
{
    static size_t getReserveLengthForElement() { return 16; } /// Just a guess.

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = size;

        Pos pos = data;
        Pos end = pos + size;

        if ((pos = find_last_symbols_or_null<'/', '\\'>(pos, end)))
        {
            ++pos;
            res_data = pos;
            res_size = end - pos;
        }
    }
};

struct NameBasename { static constexpr auto name = "basename"; };
using FunctionBasename = FunctionStringToString<ExtractSubstringImpl<ExtractBasename>, NameBasename>;

REGISTER_FUNCTION(Basename)
{
    FunctionDocumentation::Description description = R"(
Extracts the tail of a string following its last slash or backslash.
This function is often used to extract the filename from a path.
    )";
    FunctionDocumentation::Syntax syntax = "basename(expr)";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "A string expression. Backslashes must be escaped.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the tail of the input string after its last slash or backslash. If the input string ends with a slash or backslash, the function returns an empty string. Returns the original string if there are no slashes or backslashes.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Extract filename from Unix path",
        R"(
SELECT 'some/long/path/to/file' AS a, basename(a)
        )",
        R"(
┌─a──────────────────────┬─basename('some/long/path/to/file')─┐
│ some/long/path/to/file │ file                               │
└────────────────────────┴────────────────────────────────────┘
        )"
    },
    {
        "Extract filename from Windows path",
        R"(
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
        )",
        R"(
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
        )"
    },
    {
        "String with no path separators",
        R"(
SELECT 'some-file-name' AS a, basename(a)
        )",
        R"(
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBasename>(documentation);
}

}
