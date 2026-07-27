#include <Columns/ColumnConst.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Functions/FunctionFactory.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}


/** Functions that split strings into an array of strings or vice versa.
  *
  * splitByChar(sep, s[, max_substrings])
  */
namespace
{

using Pos = const char *;

class SplitByCharImpl
{
private:
    Pos pos;
    Pos end;
    char separator;
    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = "splitByChar";
    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {0, 2}; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        checkArgumentsWithSeparatorAndOptionalMaxSubstrings(func, arguments);
    }

    static constexpr auto strings_argument_position = 1uz;

    void init(const ColumnsWithTypeAndName & arguments, bool max_substrings_includes_remaining_string_)
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());

        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}. "
                "Must be constant string.", arguments[0].column->getName(), name);

        String sep_str = col->getValue<String>();

        if (sep_str.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal separator for function {}. Must be exactly one byte.", name);

        separator = sep_str[0];

        max_substrings_includes_remaining_string = max_substrings_includes_remaining_string_;
        max_splits = extractMaxSplits(arguments, 2);
    }

    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        splits = 0;
    }

    bool get(Pos & token_begin, Pos & token_end)
    {
        if (!pos)
            return false;

        token_begin = pos;

        if (max_splits)
        {
            if (max_substrings_includes_remaining_string)
            {
                if (splits == *max_splits - 1)
                {
                    token_end = end;
                    pos = nullptr;
                    return true;
                }
            }
            else
               if (splits == *max_splits)
                   return false;
        }

        pos = reinterpret_cast<Pos>(memchr(pos, separator, end - pos));
        if (pos)
        {
            token_end = pos;
            ++pos;
            ++splits;
        }
        else
            token_end = end;

        return true;
    }
};

using FunctionSplitByChar = FunctionTokens<SplitByCharImpl>;

}

REGISTER_FUNCTION(SplitByChar)
{
    FunctionDocumentation::Description description = R"(
Splits a string separated by a specified constant string `separator` of exactly one character into an array of substrings.
Empty substrings may be selected if the separator occurs at the beginning or end of the string, or if there are multiple consecutive separators.

:::note
Setting [`splitby_max_substrings_includes_remaining_string`](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) (default: `0`) controls if the remaining string is included in the last element of the result array when argument `max_substrings > 0`.
:::

Empty substrings may be selected when:
- A separator occurs at the beginning or end of the string
- There are multiple consecutive separators
- The original string `s` is empty
)";
    FunctionDocumentation::Syntax syntax = "splitByChar(separator, s[, max_substrings])";
    FunctionDocumentation::Arguments arguments = {
        {"separator", "The separator must be a single-byte character.", {"String"}},
        {"s", "The string to split.", {"String"}},
        {"max_substrings", "Optional. If `max_substrings > 0`, the returned array will contain at most `max_substrings` substrings, otherwise the function will return as many substrings as possible. The default value is `0`. ", {"Int64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of selected substrings.", {"Array(String)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT splitByChar(',', '1,2,3,abcde');",
        R"(
┌─splitByChar(⋯2,3,abcde')─┐
│ ['1','2','3','abcde']    │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSplitting;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSplitByChar>(documentation);
}

}
