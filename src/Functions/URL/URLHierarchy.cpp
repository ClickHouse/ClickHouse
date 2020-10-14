#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class URLPathHierarchyImpl
{
private:
    Pos begin;
    Pos pos;
    Pos end;
    Pos start;

public:
    static constexpr auto name = "URLPathHierarchy";
    static String getName() { return name; }

    static size_t getNumberOfArguments() { return 1; }

    static void checkArguments(const DataTypes & arguments)
    {
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void init(ColumnsWithTypeAndName & /*columns*/, const ColumnNumbers & /*arguments*/) {}

    /// Returns the position of the argument that is the column of rows
    static size_t getStringsArgumentPosition()
    {
        return 0;
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        begin = pos = pos_;
        start = begin;
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
             * (http, file - fit, mailto, magnet - do not fit), and after two slashes still at least something is there.
             * For the rest, just return an empty array.
             */
            if (pos == begin || pos == end || !(*pos++ == ':' && pos < end && *pos++ == '/' && pos < end && *pos++ == '/' && pos < end))
            {
                pos = end;
                return false;
            }

            /// The domain for simplicity is everything that after the protocol and the two slashes, until the next slash or `?` or `#`
            while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
                ++pos;

            start = pos;

            if (pos != end)
                ++pos;
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

        token_begin = start;
        token_end = pos;

        return true;
    }
};


struct NameURLPathHierarchy { static constexpr auto name = "URLPathHierarchy"; };
using FunctionURLPathHierarchy = FunctionTokens<URLPathHierarchyImpl>;

void registerFunctionURLPathHierarchy(FunctionFactory & factory)
{
    factory.registerFunction<FunctionURLPathHierarchy>();
}

}
