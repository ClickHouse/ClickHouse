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
    FunctionDocumentation::Description description = R"(
Returns the first line of a multi-line string.
)";
    FunctionDocumentation::Syntax syntax = "firstLine(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "Input string.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the first line of the input string or the whole string if there are no line separators.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(SELECT firstLine('foo\\nbar\\nbaz'))",
        R"(
┌─firstLine('foo\nbar\nbaz')─┐
│ foo                        │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFirstLine>(documentation);
}
}
