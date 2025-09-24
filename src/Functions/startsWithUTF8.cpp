#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionStartsWithUTF8 = FunctionStartsEndsWith<NameStartsWithUTF8>;
using FunctionStartsWithCaseInsensitiveUTF8 = FunctionStartsEndsWith<NameStartsWithCaseInsensitiveUTF8>;

REGISTER_FUNCTION(StartsWithUTF8)
{
    FunctionDocumentation::Description description = R"(
Checks if a string starts with the provided prefix.
Assumes that the string contains valid UTF-8 encoded text.
If this assumption is violated, no exception is thrown and the result is undefined.
)";
    FunctionDocumentation::Syntax syntax = "startsWithUTF8(s, prefix)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "String to check.", {"String"}},
        {"prefix", "Prefix to check for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `s` starts with `prefix`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT startsWithUTF8('приставка', 'при')",
        R"(
┌─startsWithUT⋯ка', 'при')─┐
│                        1 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionStartsWithUTF8>(documentation);
}

REGISTER_FUNCTION(StartsWithCaseInsensitiveUTF8)
{
    FunctionDocumentation::Description description = R"(
Checks if a string starts with the provided case-insensitive prefix.
Assumes that the string contains valid UTF-8 encoded text.
If this assumption is violated, no exception is thrown and the result is undefined.
)";
    FunctionDocumentation::Syntax syntax = "startsWithCaseInsensitiveUTF8(s, prefix)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "String to check.", {"String"}},
        {"prefix", "Case-insensitive prefix to check for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `s` starts with case-insensitive `prefix`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT startsWithCaseInsensitiveUTF8('приставка', 'при')",
        R"(
┌─startsWithUT⋯ка', 'при')─┐
│                        1 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionStartsWithCaseInsensitiveUTF8>(documentation);
}

}
