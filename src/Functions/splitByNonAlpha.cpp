#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Functions/FunctionFactory.h>
#include <Functions/Regexps.h>
#include <Interpreters/Context.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

/** Functions that split strings into an array of strings or vice versa.
  *
  * splitByNonAlpha(s[, max_substrings])        - split the string by whitespace and punctuation characters
  */
namespace
{

using Pos = const char *;

class SplitByNonAlphaImpl
{
private:
    Pos pos;
    Pos end;
    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    /// Get the name of the function.
    static constexpr auto name = "splitByNonAlpha";

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
        while (pos < end && (isWhitespaceASCII(*pos) || isPunctuationASCII(*pos)))
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

        while (pos < end && !(isWhitespaceASCII(*pos) || isPunctuationASCII(*pos)))
            ++pos;

        token_end = pos;
        splits++;

        return true;
    }
};

using FunctionSplitByNonAlpha = FunctionTokens<SplitByNonAlphaImpl>;

}

REGISTER_FUNCTION(SplitByNonAlpha)
{
    factory.registerFunction<FunctionSplitByNonAlpha>();
}

}
