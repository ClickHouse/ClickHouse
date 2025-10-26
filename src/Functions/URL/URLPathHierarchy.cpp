#include <Functions/FunctionFactory.h>
#include <Functions/FunctionTokens.h>

namespace DB
{

namespace
{

using Pos = const char *;

class URLHierarchyImpl
{
private:
    Pos begin;
    Pos pos;
    Pos end;

public:
    static constexpr auto name = "URLHierarchy";

    static bool isVariadic() { return false; }
    static size_t getNumberOfArguments() { return 1; }

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {}; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        FunctionArgumentDescriptors mandatory_args{
            {"URL", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };

        validateFunctionArguments(func, arguments, mandatory_args);
    }

    static constexpr auto strings_argument_position = 0uz;

    void init(const ColumnsWithTypeAndName & /*arguments*/, bool /*max_substring_behavior*/) {}

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        begin = pos = pos_;
        end = end_;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        /// Code from URLParser.
        if (pos == end)
            return false;

        if (pos == begin)
        {
            /// Let's parse everything that goes before the path

            /// Assume that the protocol has already been changed to lowercase.
            while (pos < end && ((*pos > 'a' && *pos < 'z') || (*pos > '0' && *pos < '9')))
                ++pos;

            /** We will calculate the hierarchy only for URLs in which there is a protocol, and after it there are two slashes.
             * (http, file - fit, mailto, magnet - do not fit), and after two slashes still at least something is there
             * For the rest, simply return the full URL as the only element of the hierarchy.
             */
            if (pos == begin || pos == end || !(pos + 3 < end && pos[0] == ':' && pos[1] == '/' && pos[2] == '/'))
            {
                pos = end;
                token_begin = begin;
                token_end = end;
                return true;
            }
            pos += 3;

            /// The domain for simplicity is everything that after the protocol and two slashes, until the next slash or `?` or `#`
            while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
                ++pos;

            if (pos != end)
                ++pos;

            token_begin = begin;
            token_end = pos;

            return true;
        }

        /// We go to the next `/` or `?` or `#`, skipping all those at the beginning.
        while (pos < end && (*pos == '/' || *pos == '?' || *pos == '#'))
            ++pos;
        if (pos == end)
            return false;
        while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
            ++pos;

        if (pos != end)
            ++pos;

        token_begin = begin;
        token_end = pos;

        return true;
    }
};


using FunctionURLHierarchy = FunctionTokens<URLHierarchyImpl>;

}

REGISTER_FUNCTION(URLHierarchy)
{
    factory.registerFunction<FunctionURLHierarchy>();
}

}
