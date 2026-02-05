#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NamePosition
{
    static constexpr auto name = "position";
};

using FunctionPosition = FunctionsStringSearch<PositionImpl<NamePosition, PositionCaseSensitiveASCII>>;

}

REGISTER_FUNCTION(Position)
{
    FunctionDocumentation::Description description = R"(
Returns the position (in bytes, starting at 1) of a substring `needle` in a string `haystack`.

If substring `needle` is empty, these rules apply:
- if no `start_pos` was specified: return `1`
- if `start_pos = 0`: return `1`
- if `start_pos >= 1` and `start_pos <= length(haystack) + 1`: return `start_pos`
- otherwise: return `0`

The same rules also apply to functions [`locate`](#locate), [`positionCaseInsensitive`](#positionCaseInsensitive), [`positionUTF8`](#positionUTF8) and [`positionCaseInsensitiveUTF8`](#positionCaseInsensitiveUTF8).
    )";
    FunctionDocumentation::Syntax syntax = "position(haystack, needle[, start_pos])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String", "Enum"}},
        {"needle", "Substring to be searched.", {"String"}},
        {"start_pos", "Position (1-based) in `haystack` at which the search starts. Optional.", {"UInt"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns starting position in bytes and counting from 1, if the substring was found, otherwise `0`, if the substring was not found.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        "SELECT position('Hello, world!', '!')",
        R"(
┌─position('Hello, world!', '!')─┐
│                             13 │
└────────────────────────────────┘
        )"
    },
    {
        "With start_pos argument",
        "SELECT position('Hello, world!', 'o', 1), position('Hello, world!', 'o', 7)",
        R"(
┌─position('Hello, world!', 'o', 1)─┬─position('Hello, world!', 'o', 7)─┐
│                                 5 │                                 9 │
└───────────────────────────────────┴───────────────────────────────────┘
        )"
    },
    {
        "Needle IN haystack syntax",
        "SELECT 6 = position('/' IN s) FROM (SELECT 'Hello/World' AS s)",
        R"(
┌─equals(6, position(s, '/'))─┐
│                           1 │
└─────────────────────────────┘
        )"
    },
    {
        "Empty needle substring",
        "SELECT position('abc', ''), position('abc', '', 0), position('abc', '', 1), position('abc', '', 2), position('abc', '', 3), position('abc', '', 4), position('abc', '', 5)",
        R"(
┌─position('abc', '')─┬─position('abc', '', 0)─┬─position('abc', '', 1)─┬─position('abc', '', 2)─┬─position('abc', '', 3)─┬─position('abc', '', 4)─┬─position('abc', '', 5)─┐
│                   1 │                      1 │                      1 │                      2 │                      3 │                      4 │                      0 │
└─────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPosition>(documentation, FunctionFactory::Case::Insensitive);
}
}
