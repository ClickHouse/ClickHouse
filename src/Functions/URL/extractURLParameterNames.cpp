#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class ExtractURLParameterNamesImpl
{
private:
    Pos pos;
    Pos end;
    bool first;

public:
    static constexpr auto name = "extractURLParameterNames";
    static String getName() { return name; }

    static bool isVariadic() { return false; }
    static size_t getNumberOfArguments() { return 1; }

    static void checkArguments(const DataTypes & arguments)
    {
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// Returns the position of the argument that is the column of rows
    static size_t getStringsArgumentPosition()
    {
        return 0;
    }

    void init(const ColumnsWithTypeAndName & /*arguments*/) {}

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
        if (pos == nullptr)
            return false;

        if (first)
        {
            first = false;
            pos = find_first_symbols<'?', '#'>(pos, end);
        }
        else
            pos = find_first_symbols<'&', '#'>(pos, end);

        if (pos + 1 >= end)
            return false;
        ++pos;

        while (true)
        {
            token_begin = pos;

            pos = find_first_symbols<'=', '&', '#', '?'>(pos, end);
            if (pos == end)
                return false;
            else
                token_end = pos;

            if (*pos == '?')
            {
                ++pos;
                continue;
            }

            break;
        }

        return true;
    }
};

struct NameExtractURLParameterNames { static constexpr auto name = "extractURLParameterNames"; };
using FunctionExtractURLParameterNames = FunctionTokens<ExtractURLParameterNamesImpl>;

void registerFunctionExtractURLParameterNames(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractURLParameterNames>();
}

}
