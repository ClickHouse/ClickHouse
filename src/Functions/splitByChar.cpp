#include <Columns/ColumnConst.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Functions/FunctionFactory.h>
#include <Common/StringUtils/StringUtils.h>
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
    std::optional<size_t> result_reserve_size;

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

        const ColumnString * col_str = checkAndGetColumn<ColumnString>(arguments[strings_argument_position].column.get());
        /// There is another possibility that the input column is ColumnConst. We ignore it because there is no need to get reserve size under such condition.
        if (col_str)
        {
            const ColumnString::Chars & src_chars = col_str->getChars();
            /// Consider use case: splitByChar(' ', 'a b c'), where input chars is "a b c\0", output chars is "a\0", "b\0", "c\0".
            /// The size of output chars should never exceeds input chars
            result_reserve_size = src_chars.size();
        }
    }

    std::optional<size_t> getResultReserveSize() const
    {
        return result_reserve_size;
    }

    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        splits = 0;
    }

    bool get(Pos & token_begin, Pos & token_end)
    {
        if (!pos) [[unlikely]]
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
    factory.registerFunction<FunctionSplitByChar>();
}

}
