#include <Functions/FunctionTokens.h>
#include <Functions/FunctionFactory.h>
#include <Common/StringUtils.h>


namespace DB
{

/** Functions that split strings into an array of strings or vice versa.
  *
  * splitByWhitespace(s[, max_substrings])      - split the string by whitespace characters
  */
namespace
{

using Pos = const char *;

class SplitByWhitespaceImpl
{
private:
    Pos pos;
    Pos end;
    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = "splitByWhitespace";

    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {1}; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        checkArgumentsWithOptionalMaxSubstrings(func, arguments);
    }

    static constexpr auto strings_argument_position = 0uz;

    void init(const ColumnsWithTypeAndName & arguments, bool max_substrings_includes_remaining_string_)
    {
        max_substrings_includes_remaining_string = max_substrings_includes_remaining_string_;
        max_splits = extractMaxSplits(arguments, 1);
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        splits = 0;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        /// Skip garbage
        while (pos < end && isWhitespaceASCII(*pos))
            ++pos;

        if (pos == end)
            return false;

        token_begin = pos;

        if (max_splits)
        {
            if (max_substrings_includes_remaining_string)
            {
                if (splits == *max_splits - 1)
                {
                    token_end = end;
                    pos = end;
                    return true;
                }
            }
            else
                if (splits == *max_splits)
                    return false;
        }

        while (pos < end && !isWhitespaceASCII(*pos))
            ++pos;

        token_end = pos;
        splits++;

        return true;
    }
};

using FunctionSplitByWhitespace = FunctionTokens<SplitByWhitespaceImpl>;

}

REGISTER_FUNCTION(SplitByWhitespace)
{
    FunctionDocumentation::Description description = R"(
Splits a string which is separated by whitespace characters into an array of substrings.

:::note
Setting [`splitby_max_substrings_includes_remaining_string`](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) (default: `0`) controls if the remaining string is included in the last element of the result array when argument `max_substrings > 0`.
:::
)";
    FunctionDocumentation::Syntax syntax = "splitByWhitespace(s[, max_substrings])";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to split.", {"String"}},
        {"max_substrings", "Optional. When `max_substrings > 0`, the returned substrings will be no more than `max_substrings`, otherwise the function will return as many substrings as possible. Default value: `0`.", {"Int64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of the selected substrings of `s`.", {"Array(String)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT splitByWhitespace('  1!  a,  b.  ');",
        R"(
['1!','a,','b.']
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSplitting;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSplitByWhitespace>(documentation);
}

}
