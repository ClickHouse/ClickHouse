#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/StringHelpers.h>
#include <base/find_symbols.h>

namespace DB
{

struct FirstLine
{
    static size_t getReserveLengthForElement() { return 16; }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;

        const Pos end = data + size;
        const Pos pos = find_first_symbols<'\r', '\n'>(data, end);
        res_size = pos - data;
    }
};

struct NameFirstLine
{
    static constexpr auto name = "firstLine";
};

using FunctionFirstLine = FunctionStringToString<ExtractSubstringImpl<FirstLine>, NameFirstLine>;

REGISTER_FUNCTION(FirstLine)
{
    factory.registerFunction<FunctionFirstLine>(FunctionDocumentation{
        .description = "Returns first line of a multi-line string.",
        .syntax = "firstLine(string)",
        .arguments = {{.name = "string", .description = "The string to process."}},
        .returned_value = {"The first line of the string or the whole string if there is no line separators."},
        .examples = {
            {.name = "Return first line", .query = "firstLine('Hello\\nWorld')", .result = "'Hello'"},
            {.name = "Return whole string", .query = "firstLine('Hello World')", .result = "'Hello World'"},
        }});
}
}
