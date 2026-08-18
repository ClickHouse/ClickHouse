#include <Functions/FunctionFactory.h>
#include <Functions/FunctionTokens.h>


namespace DB
{

namespace
{

using Pos = const char *;

class ExtractURLParametersImpl
{
private:
    Pos pos;
    Pos end;
    bool first;

public:
    static constexpr auto name = "extractURLParameters";

    static bool isVariadic() { return false; }
    static size_t getNumberOfArguments() { return 1; }

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {}; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        FunctionArgumentDescriptors mandatory_args
        {
            {"URL", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };

        validateFunctionArguments(func, arguments, mandatory_args);
    }

    void init(const ColumnsWithTypeAndName & /*arguments*/, bool /*max_substrings_includes_remaining_string*/) {}

    static constexpr auto strings_argument_position = 0uz;

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        first = true;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        if (pos == end)
            return false;

        /// Skip to the query string or fragment identifier
        if (first)
        {
            first = false;
            pos = find_first_symbols<'?', '#'>(pos, end);
            if (pos + 1 >= end)
                return false;
            ++pos;
        }

        /// Find either parameter name (&, #) or value (=)
        while (true)
        {
            token_begin = pos;
            pos = find_first_symbols<'=', '&', '#', '?'>(pos, end);
            if (pos == end)
                return false;

            if (*pos == '?')
            {
                ++pos;
                continue;
            }

            break;
        }

        if (pos < end && (*pos == '&' || *pos == '#'))
        {
            /// The end of the current parameter
            token_end = pos;
            ++pos;
        }
        else if (pos + 1 >= end)
        {
            token_end = end;
            ++pos;
        }
        else
        {
            ++pos;
            pos = find_first_symbols<'&', '#'>(pos, end);
            if (pos == end)
            {
                token_end = end;
            }
            else
            {
                token_end = pos;
                ++pos;
            }
        }

        return true;
    }
};

using FunctionExtractURLParameters = FunctionTokens<ExtractURLParametersImpl>;

}

REGISTER_FUNCTION(ExtractURLParameters)
{
    factory.registerFunction<FunctionExtractURLParameters>();
}

}
