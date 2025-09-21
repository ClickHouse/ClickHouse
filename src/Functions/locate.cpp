#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameLocate
{
    static constexpr auto name = "locate";
};

using FunctionLocate = FunctionsStringSearch<PositionImpl<NameLocate, PositionCaseSensitiveASCII>, ExecutionErrorPolicy::Throw, HaystackNeedleOrderIsConfigurable::Yes>;

}

REGISTER_FUNCTION(Locate)
{
    FunctionDocumentation::Description description = R"(
Like [`position`](#position) but with arguments `haystack` and `locate` switched.

:::note Version dependent behavior
The behavior of this function depends on the ClickHouse version:
- in versions < v24.3, `locate` was an alias of function `position` and accepted arguments `(haystack, needle[, start_pos])`.
- in versions >= 24.3, `locate` is an individual function (for better compatibility with MySQL) and accepts arguments `(needle, haystack[, start_pos])`.
The previous behavior can be restored using setting `function_locate_has_mysql_compatible_argument_order = false`.
:::
    )";
    FunctionDocumentation::Syntax syntax = "locate(needle, haystack[, start_pos])";
    FunctionDocumentation::Arguments arguments = {
        {"needle", "Substring to be searched.", {"String"}},
        {"haystack", "String in which the search is performed.", {"String", "Enum"}},
        {"start_pos", "Optional. Position (1-based) in `haystack` at which the search starts.", {"UInt"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns starting position in bytes and counting from 1, if the substring was found, `0`, if the substring was not found.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        "SELECT locate('ca', 'abcabc')",
        R"(
┌─locate('ca', 'abcabc')─┐
│                      3 │
└────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 16};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLocate>(documentation, FunctionFactory::Case::Insensitive);
}
}
