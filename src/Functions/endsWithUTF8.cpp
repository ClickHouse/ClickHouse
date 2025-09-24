#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionEndsWithUTF8 = FunctionStartsEndsWith<NameEndsWithUTF8>;
using FunctionEndsWithUTF8CaseInsensitive = FunctionStartsEndsWith<NameEndsWithUTF8CaseInsensitive>;

REGISTER_FUNCTION(EndsWithUTF8)
{
    FunctionDocumentation::Description description = R"(
Returns whether string `s` ends with `suffix`.
Assumes that the string contains valid UTF-8 encoded text.
If this assumption is violated, no exception is thrown and the result is undefined.
)";
    FunctionDocumentation::Syntax syntax = "endsWithUTF8(s, suffix)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "String to check.", {"String"}},
        {"suffix", "Suffix to check for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `s` ends with `suffix`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT endsWithUTF8('данных', 'ых');",
        R"(
┌─endsWithUTF8('данных', 'ых')─┐
│                            1 │
└──────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionEndsWithUTF8>(documentation);
}

REGISTER_FUNCTION(EndsWithUTF8CaseInsensitive)
{
    FunctionDocumentation::Description description = R"(
Returns whether string `s` ends with case-insensitive `suffix`.
Assumes that the string contains valid UTF-8 encoded text.
If this assumption is violated, no exception is thrown and the result is undefined.
)";
    FunctionDocumentation::Syntax syntax = "endsWithUTF8CaseInsensitive(s, suffix)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "String to check.", {"String"}},
        {"suffix", "Case-insensitive suffix to check for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `s` ends with case-insensitive `suffix`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT endsWithUTF8CaseInsensitive('данных', 'ых');",
        R"(
┌─endsWithUTF8CaseInsensitive('данных', 'ых')─┐
│                                           1 │
└─────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionEndsWithUTF8CaseInsensitive>(documentation);
}
}
