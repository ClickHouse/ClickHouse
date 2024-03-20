
#include <Functions/FunctionTokens.h>
#include <Functions/FunctionFactory.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

/** Functions that split strings into an array of strings or vice versa.
  *
  * alphaTokens(s[, max_substrings])            - select from the string subsequence `[a-zA-Z]+`.
  */
namespace
{

using Pos = const char *;

class SplitByAlphaImpl
{
private:
    Pos pos;
    Pos end;
    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = "alphaTokens";

    static bool isVariadic() { return true; }

    static size_t getNumberOfArguments() { return 0; }

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
        while (pos < end && !isAlphaASCII(*pos))
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

        while (pos < end && isAlphaASCII(*pos))
            ++pos;

        token_end = pos;
        ++splits;

        return true;
    }
};

using FunctionSplitByAlpha = FunctionTokens<SplitByAlphaImpl>;

}

REGISTER_FUNCTION(SplitByAlpha)
{
    factory.registerFunction<FunctionSplitByAlpha>();
    factory.registerAlias("splitByAlpha", FunctionSplitByAlpha::name);
}

}
