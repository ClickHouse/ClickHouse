#include <Functions/FunctionFactory.h>
#include <Functions/toFixedString.h>


namespace DB
{

REGISTER_FUNCTION(FixedString)
{
    /// toFixedString documentation
    FunctionDocumentation::Description description = R"(
Converts a [`String`](/reference/data-types/string) argument to a [`FixedString(N)`](/reference/data-types/fixedstring) type (a string of fixed length N).

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
-- The padding is made of null bytes, so the result is shown with `hex`.
SELECT hex(toFixedString('foo', 8)) AS s;
        )",
        R"(
┌─s────────────────┐
│ 666F6F0000000000 │
└──────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToFixedString>(documentation);
}

}
