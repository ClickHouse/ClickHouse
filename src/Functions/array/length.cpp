#include <Functions/array/length.h>

namespace DB
{

REGISTER_FUNCTION(Length)
{
    FunctionDocumentation::Description description = R"(
Calculates the length of a string or array.

- For String or FixedString arguments: calculates the number of bytes in the string.
- For Array arguments: calculates the number of elements in the array.
- For QBit arguments: calculates the dimension of the vector.
- If applied to a FixedString or QBit argument, the function is a constant expression.

Please note that the number of bytes in a string is not the same as the number of
Unicode "code points" and it is not the same as the number of Unicode "grapheme clusters"
(what we usually call "characters") and it is not the same as the visible string width.

It is ok to have ASCII NULL bytes in strings, and they will be counted as well.
    )";
    FunctionDocumentation::Syntax syntax = "length(x)";
    FunctionDocumentation::Arguments arguments = {{"x", "Value for which to calculate the number of bytes (for String/FixedString), elements (for Array), or the dimension (for QBit).", {"String", "FixedString", "Array(T)", "QBit"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of bytes in the String/FixedString `x`, the number of elements in array `x`, or the dimension of the QBit `x`", {"UInt64"}};
    FunctionDocumentation::Examples examples {
    {
        "String example",
        "SELECT length('Hello, world!')",
        "13"
    },
    {
        "Array example",
        "SELECT length(['Hello', 'world'])",
        "2"
    },
    {
        "QBit example",
        "SELECT length([0, 0, 0, 0, 0, 0, 0, 0]::QBit(Float32, 8))",
        "8"
    },
    {
        "constexpr example",
        R"(
WITH 'hello' || toString(number) AS str
SELECT str,
isConstant(length(str)) AS str_length_is_constant,
isConstant(length(str::FixedString(6))) AS fixed_str_length_is_constant
FROM numbers(3)
        )",
        R"(
┌─str────┬─str_length_is_constant─┬─fixed_str_length_is_constant─┐
│ hello0 │                      0 │                            1 │
│ hello1 │                      0 │                            1 │
│ hello2 │                      0 │                            1 │
└────────┴────────────────────────┴──────────────────────────────┘
        )"
    },
    {
        "unicode example",
        "SELECT 'ёлка' AS str1, length(str1), lengthUTF8(str1), normalizeUTF8NFKD(str1) AS str2, length(str2), lengthUTF8(str2)",
        R"(
┌─str1─┬─length(str1)─┬─lengthUTF8(str1)─┬─str2─┬─length(str2)─┬─lengthUTF8(str2)─┐
│ ёлка │            8 │                4 │ ёлка │           10 │                5 │
└──────┴──────────────┴──────────────────┴──────┴──────────────┴──────────────────┘
        )"
    },
    {
        "ascii_vs_utf8 example",
        "SELECT 'ábc' AS str, length(str), lengthUTF8(str)",
        R"(
┌─str─┬─length(str)─┬─lengthUTF8(str)─┐
│ ábc │           4 │               3 │
└─────┴─────────────┴─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLength>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("OCTET_LENGTH", "length", FunctionFactory::Case::Insensitive);
    /// `CARDINALITY` is a SQL-standard / PostgreSQL spelling for the array-length operator.
    /// In ClickHouse it is a full alias of `length`, so it inherits `length`'s behavior for non-array arguments too
    /// (e.g. it returns the byte length of strings); this is a ClickHouse extension beyond the standard.
    factory.registerAlias("CARDINALITY", "length", FunctionFactory::Case::Insensitive);
}

}
