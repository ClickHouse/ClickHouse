#include <Functions/FunctionFactory.h>
#include <Functions/LeftRight.h>

namespace DB
{

REGISTER_FUNCTION(Right)
{
    FunctionDocumentation::Description description = R"(
Returns a substring of string `s` with a specified `offset` starting from the right.
)";
    FunctionDocumentation::Syntax syntax = "right(s, offset)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to calculate a substring from.", {"String", "FixedString"}},
        {"offset", "The number of bytes of the offset.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns:
- For positive `offset`, a substring of `s` with `offset` many bytes, starting from the right of the string.
- For negative `offset`, a substring of `s` with `length(s) - |offset|` bytes, starting from the right of the string.
- An empty string if `length` is `0`.
    )", {"String"}};
    FunctionDocumentation::Examples examples = {
        {"Positive offset", "SELECT right('Hello', 3)", "llo"},
        {"Negative offset", "SELECT right('Hello', -3)", "lo"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    FunctionDocumentation::Description description_utf8 = R"(
Returns a substring of UTF-8 encoded string `s` with a specified `offset` starting from the right.
)";
    FunctionDocumentation::Syntax syntax_utf8 = "rightUTF8(s, offset)";
    FunctionDocumentation::Arguments arguments_utf8 = {
        {"s", "The UTF-8 encoded string to calculate a substring from.", {"String", "FixedString"}},
        {"offset", "The number of bytes of the offset.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_utf8 = {R"(
Returns:
- For positive `offset`, a substring of `s` with `offset` many bytes, starting from the right of the string.
- For negative `offset`, a substring of `s` with `length(s) - |offset|` bytes, starting from the right of the string.
- An empty string if `length` is `0`.
    )", {"String"}};
    FunctionDocumentation::Examples examples_utf8 = {
        {"Positive offset", "SELECT rightUTF8('Привет', 4)", "ивет"},
        {"Negative offset", "SELECT rightUTF8('Привет', -4)", "ет"}
    };
    FunctionDocumentation documentation_utf8 = {description_utf8, syntax_utf8, arguments_utf8, returned_value_utf8, examples_utf8, introduced_in, category};

    factory.registerFunction<FunctionLeftRight<false, SubstringDirection::Right>>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionLeftRight<true, SubstringDirection::Right>>(documentation_utf8, FunctionFactory::Case::Sensitive);
}

}
