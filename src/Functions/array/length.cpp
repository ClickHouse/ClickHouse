#include <Functions/array/length.h>

namespace DB
{

REGISTER_FUNCTION(Length)
{
    FunctionDocumentation::Description description = R"(
Calculates the length of a string or array.

- For String or FixedString arguments: calculates the number of bytes in the string.
- For Array arguments: calculates the number of elements in the array.
- If applied to a FixedString argument, the function is a constant expression.

Please note that the number of bytes in a string is not the same as the number of
Unicode "code points" and it is not the same as the number of Unicode "grapheme clusters"
(what we usually call "characters") and it is not the same as the visible string width.

It is ok to have ASCII NUL bytes in strings, and they will be counted as well.
    )";
    FunctionDocumentation::Syntax syntax = "length(x)";
    FunctionDocumentation::Arguments arguments = {{"x", "String, FixedString or Array for which to calculate the number of bytes (for String/FixedString) or elements (for Array)."}};
    FunctionDocumentation::ReturnedValue returned_value = "Returns the number of number of bytes in the String/FixedString `x` / the number of elements in array `x`";
    FunctionDocumentation::Examples examples {
        {"string1", "SELECT length('Hello, world!')", "13"},
        {"arr1", "SELECT length(['Hello', 'world'])", "2"},
        {"constexpr", R"(
WITH 'hello' || toString(number) AS str
SELECT str,
isConstant(length(str)) AS str_length_is_constant,
isConstant(length(str::FixedString(6))) AS fixed_str_length_is_constant
FROM numbers(3)
        )", R"(
┌─str────┬─str_length_is_constant─┬─fixed_str_length_is_constant─┐
│ hello0 │                      0 │                            1 │
│ hello1 │                      0 │                            1 │
│ hello2 │                      0 │                            1 │
└────────┴────────────────────────┴──────────────────────────────┘
        )"},
        {"unicode", "SELECT 'ёлка' AS str1, length(str1), lengthUTF8(str1), normalizeUTF8NFKD(str1) AS str2, length(str2), lengthUTF8(str2)", R"(
┌─str1─┬─length(str1)─┬─lengthUTF8(str1)─┬─str2─┬─length(str2)─┬─lengthUTF8(str2)─┐
│ ёлка │            8 │                4 │ ёлка │           10 │                5 │
└──────┴──────────────┴──────────────────┴──────┴──────────────┴──────────────────┘
        )"},
        {"ascii_vs_utf8", "SELECT 'ábc' AS str, length(str), lengthUTF8(str)", R"(
┌─str─┬─length(str)──┬─lengthUTF8(str)─┐
│ ábc │            4 │               3 │
└─────┴──────────────┴─────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLength>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("OCTET_LENGTH", "length", FunctionFactory::Case::Insensitive);
}

}
