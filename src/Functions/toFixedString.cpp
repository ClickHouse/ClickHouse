#include <Functions/FunctionFactory.h>
#include <Functions/toFixedString.h>


namespace DB
{

REGISTER_FUNCTION(FixedString)
{
    /// toFixedString documentation
    FunctionDocumentation::Description description = R"(
Converts a [`String`](/sql-reference/data-types/string) argument to a [`FixedString(N)`](/sql-reference/data-types/fixedstring) type (a string of fixed length N).

If the string has fewer bytes than N, it is padded with null bytes to the right.
If the string has more bytes than N, an exception is thrown.
    )";
    FunctionDocumentation::Syntax syntax = "toFixedString(s, N)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "String to convert.", {"String"}},
        {"N", "Length of the resulting FixedString.", {"const UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a FixedString of length N.", {"FixedString(N)"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToFixedString>(documentation);
}

}
