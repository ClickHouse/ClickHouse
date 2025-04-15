#include <Functions/array/length.h>

namespace DB
{

REGISTER_FUNCTION(Length)
{
    FunctionDocumentation::Description description = R"(
    Calculates the length of the string or array.

    - For a String or FixedString argument: calculates the number of bytes in string.
    - For an Array argument: calculates the number of elements in the array.
    - If applied for a FixedString argument, the function is a constant expression.

    Please note that the number of bytes in a string is not the same as the number of Unicode "code points"
    and it is not the same as the number of Unicode "grapheme clusters" (what we usually call "characters").
    It is also not the same as the visible string width.

    It is ok to have ASCII NUL bytes in strings, and they will be counted as well.

    :::note
    Can be optimized by enabling the [`optimize_functions_to_subcolumns` setting](/operations/settings/settings#optimize_functions_to_subcolumns).
    With `optimize_functions_to_subcolumns = 1` the function reads only [size0](/sql-reference/data-types/array#array-size) subcolumn instead
    of reading and processing the whole array column. The query `SELECT empty(arr) FROM TABLE;`
    transforms to `SELECT arr.size0 = 0 FROM TABLE;`.
    :::
    )";
    FunctionDocumentation::Syntax syntax = "length(x)";
    FunctionDocumentation::Argument argument1 = {"x", "String, FixedString or Array for which to calculate the length."};
    FunctionDocumentation::Arguments arguments = {argument1};
    FunctionDocumentation::ReturnedValue returned_value = "Returns the number of bytes for String or FixedString, the number of elements for an Array.";
    FunctionDocumentation::Example example1 = {"String example", "SELECT length('Hello, world!')", "13"};
    FunctionDocumentation::Example example2 = {"Array example", "SELECT length(['Hello', 'world'])", "2"};
    FunctionDocumentation::Example example3 = {"constexpr example", R"(
    WITH 'hello' || toString(number) AS str
        SELECT
            str,
            isConstant(length(str)) AS str_length_is_constant,
            isConstant(length(str::FixedString(6))) AS fixed_str_length_is_constant
        FROM numbers(3)
    )", R"(
    ┌─str────┬─str_length_is_constant─┬─fixed_str_length_is_constant─┐
    │ hello0 │                      0 │                            1 │
    │ hello1 │                      0 │                            1 │
    │ hello2 │                      0 │                            1 │
    └────────┴────────────────────────┴──────────────────────────────┘
    )"};
    FunctionDocumentation::Example example4 = {"unicode example", R"(
    SELECT
      'ёлка' AS str1,
      length(str1),
      lengthUTF8(str1),
      normalizeUTF8NFKD(str1) AS str2,
      length(str2),
      lengthUTF8(str2)
    FORMAT Vertical)", R"(
    Row 1:
    ──────
    str1:             ёлка
    length(str1):     8
    lengthUTF8(str1): 4
    str2:             ёлка
    length(str2):     10
    lengthUTF8(str2): 5
    )"};
    FunctionDocumentation::Example example5 = {"null example", R"(
    SELECT 'abc\0\0\0' AS str, length(str)
    )", R"(
    ┌─str─┬─length(str)─┐
    │ abc │           6 │
    └─────┴─────────────┘
    )"};
    FunctionDocumentation::Examples examples = {example1, example2, example3, example4, example5};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, categories};

    factory.registerFunction<FunctionLength>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("OCTET_LENGTH", "length", FunctionFactory::Case::Insensitive);
}

}
