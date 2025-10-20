#include <Functions/FunctionFactory.h>
#include <Functions/FunctionTokens.h>

namespace DB
{

namespace
{

using Pos = const char *;

class ExtractURLParameterNamesImpl
{
private:
    Pos pos;
    Pos end;
    bool first;

public:
    static constexpr auto name = "extractURLParameterNames";

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

    void init(const ColumnsWithTypeAndName & /*arguments*/, bool /*max_substrings_includes_remaining_string*/) {}

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

using FunctionExtractURLParameterNames = FunctionTokens<ExtractURLParameterNamesImpl>;

}

REGISTER_FUNCTION(ExtractURLParameterNames)
{
    /// extractURLParameterNames documentation
    FunctionDocumentation::Description description_extractURLParameterNames = R"(
Returns an array of name strings corresponding to the names of URL parameters.
The values are not decoded.
    )";
    FunctionDocumentation::Syntax syntax_extractURLParameterNames = "extractURLParameterNames(url)";
    FunctionDocumentation::Arguments arguments_extractURLParameterNames = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_extractURLParameterNames = {"Returns an array of name strings corresponding to the names of URL parameters.", {"Array(String)"}};
    FunctionDocumentation::Examples examples_extractURLParameterNames = {
    {
        "Usage example",
        R"(
SELECT extractURLParameterNames('http://example.com/?param1=value1&param2=value2');
        )",
        R"(
┌─extractURLPa⋯m2=value2')─┐
│ ['param1','param2']      │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_extractURLParameterNames = {1, 1};
    FunctionDocumentation::Category category_extractURLParameterNames = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_extractURLParameterNames = {description_extractURLParameterNames, syntax_extractURLParameterNames, arguments_extractURLParameterNames, returned_value_extractURLParameterNames, examples_extractURLParameterNames, introduced_in_extractURLParameterNames, category_extractURLParameterNames};

    factory.registerFunction<FunctionExtractURLParameterNames>(documentation_extractURLParameterNames);
}

}
