#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionStartsWith = FunctionStartsEndsWith<NameStartsWith>;
using FunctionStartsWithCaseInsensitive = FunctionStartsEndsWith<NameStartsWithCaseInsensitive>;

REGISTER_FUNCTION(StartsWith)
{
    FunctionDocumentation::Description description = R"(
Checks whether a string begins with the provided string.
)";
    FunctionDocumentation::Syntax syntax = "startsWith(s, prefix)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "String to check.", {"String"}},
        {"prefix", "Prefix to check for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `s` starts with `prefix`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT startsWith('ClickHouse', 'Click');",
        R"(
┌─startsWith('⋯', 'Click')─┐
│                        1 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionStartsWith>(documentation);
}

REGISTER_FUNCTION(StartsWithCaseInsensitive)
{
    FunctionDocumentation::Description description = R"(
Checks whether a string begins with the provided case-insensitive string.
)";
    FunctionDocumentation::Syntax syntax = "startsWithCaseInsensitive(s, prefix)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "String to check.", {"String"}},
        {"prefix", "Case-insensitive prefix to check for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `s` starts with case-insensitive `prefix`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT startsWithCaseInsensitive('ClickHouse', 'CLICK');",
        R"(
┌─startsWithCaseInsensitive('⋯', 'CLICK')─┐
│                                       1 │
└─────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionStartsWithCaseInsensitive>(documentation);
}

}
