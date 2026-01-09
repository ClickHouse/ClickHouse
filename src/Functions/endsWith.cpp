#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionEndsWith = FunctionStartsEndsWith<NameEndsWith>;

REGISTER_FUNCTION(EndsWith)
{
    FunctionDocumentation::Description description = R"(
Checks whether a string ends with the provided suffix.
)";
    FunctionDocumentation::Syntax syntax = "endsWith(s, suffix)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "String to check.", {"String"}},
        {"suffix", "Suffix to check for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `s` ends with `suffix`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT endsWith('ClickHouse', 'House');",
        R"(
┌─endsWith('Cl⋯', 'House')─┐
│                        1 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionEndsWith>(documentation);
}

}

