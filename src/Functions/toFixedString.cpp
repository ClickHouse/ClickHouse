#include <Functions/FunctionFactory.h>
#include <Functions/toFixedString.h>


namespace DB
{

REGISTER_FUNCTION(FixedString)
{
    /// toFixedString documentation
    FunctionDocumentation::Description toFixedString_description = R"(
Converts a [`String`](/sql-reference/data-types/string) argument to a [`FixedString(N)`](/sql-reference/data-types/fixedstring) type (a string of fixed length N).

If the string has fewer bytes than N, it is padded with null bytes to the right.
If the string has more bytes than N, an exception is thrown.
    )";
    FunctionDocumentation::Syntax toFixedString_syntax = "toFixedString(s, N)";
    FunctionDocumentation::Arguments toFixedString_arguments = {
        {"s", "String to convert.", {"String"}},
        {"N", "Length of the resulting FixedString.", {"const UInt*"}}
    };
    FunctionDocumentation::ReturnedValue toFixedString_returned_value = {"Returns a FixedString of length N.", {"FixedString(N)"}};
    FunctionDocumentation::Examples toFixedString_examples = {
    {
        "Usage example",
        R"(
SELECT toFixedString('foo', 8) AS s;
        )",
        R"(
┌─s─────────────┐
│ foo\0\0\0\0\0 │
└───────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toFixedString_introduced_in = {1, 1};
    FunctionDocumentation::Category toFixedString_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toFixedString_documentation = {toFixedString_description, toFixedString_syntax, toFixedString_arguments, toFixedString_returned_value, toFixedString_examples, toFixedString_introduced_in, toFixedString_category};

    factory.registerFunction<FunctionToFixedString>(toFixedString_documentation);
}

}
