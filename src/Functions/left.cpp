#include <Functions/FunctionFactory.h>
#include <Functions/LeftRight.h>

namespace DB
{

REGISTER_FUNCTION(Left)
{
    FunctionDocumentation::Description description = R"(
Returns a substring of string `s` with a specified `offset` starting from the left.
)";
    FunctionDocumentation::Syntax syntax = "left(s, offset)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to calculate a substring from.", {"String", "FixedString"}},
        {"offset", "The number of bytes of the offset.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns:
- For positive `offset`, a substring of `s` with `offset` many bytes, starting from the left of the string.
- For negative `offset`, a substring of `s` with `length(s) - |offset|` bytes, starting from the left of the string.
- An empty string if `length` is `0`.
    )",
    {"String"}
    };
    FunctionDocumentation::Examples examples = {
        {"Positive offset", "SELECT left('Hello World', 5)", "Helllo"},
        {"Negative offset", "SELECT left('Hello World', -6)", "Hello"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    FunctionDocumentation::Description description_utf8 = R"(
Returns a substring of a UTF-8-encoded string `s` with a specified `offset` starting from the left.
)";
    FunctionDocumentation::Syntax syntax_utf8 = "leftUTF8(s, offset)";
    FunctionDocumentation::Arguments arguments_utf8 = {
        {"s", "The UTF-8 encoded string to calculate a substring from.", {"String", "FixedString"}},
        {"offset", "The number of bytes of the offset.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_utf8 = {R"(
Returns:
- For positive `offset`, a substring of `s` with `offset` many bytes, starting from the left of the string.\n"
- For negative `offset`, a substring of `s` with `length(s) - |offset|` bytes, starting from the left of the string.\n"
- An empty string if `length` is 0.
    )",
    {"String"}
    };
    FunctionDocumentation::Examples examples_utf8 = {
        {"Positive offset", "SELECT leftUTF8('Привет', 4)", "Прив"},
        {"Negative offset", "SELECT leftUTF8('Привет', -4)", "Пр"}
    };
    FunctionDocumentation documentation_utf8 = {description_utf8, syntax_utf8, arguments_utf8, returned_value_utf8, examples_utf8, introduced_in, category};

    factory.registerFunction<FunctionLeftRight<false, SubstringDirection::Left>>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionLeftRight<true, SubstringDirection::Left>>(documentation_utf8, FunctionFactory::Case::Sensitive);
}

}
