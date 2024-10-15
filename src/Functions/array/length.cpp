#include <Functions/array/length.h>

namespace DB
{

REGISTER_FUNCTION(Length)
{
    factory.registerFunction<FunctionLength>(
        FunctionDocumentation{
            .description=R"(
Calculates the length of the string or array.

For String or FixedString argument: calculates the number of bytes in string.
[example:string1]

For Array argument: calculates the number of elements in the array.
[example:arr1]

If applied for FixedString argument, the function is a constant expression:
[example:constexpr]

Please note that the number of bytes in a string is not the same as the number of Unicode "code points"
and it is not the same as the number of Unicode "grapheme clusters" (what we usually call "characters")
and it is not the same as the visible string width.
[example:unicode]

It is ok to have ASCII NUL bytes in strings, and they will be counted as well.
[example:nul]
)",
            .examples{
                {"string1", "SELECT length('Hello, world!')", ""},
                {"arr1", "SELECT length(['Hello'], ['world'])", ""},
                {"constexpr", "WITH 'hello' || toString(number) AS str\n"
                              "SELECT str, \n"
                              "       isConstant(length(str)) AS str_length_is_constant, \n"
                              "       isConstant(length(str::FixedString(6))) AS fixed_str_length_is_constant\n"
                              "FROM numbers(3)", ""},
                {"unicode", "SELECT 'ёлка' AS str1, length(str1), lengthUTF8(str1), normalizeUTF8NFKD(str1) AS str2, length(str2), lengthUTF8(str2)", ""},
                {"nul", R"(SELECT 'abc\0\0\0' AS str, length(str))", ""},
                },
            .categories{"String", "Array"}
        },
        FunctionFactory::Case::Insensitive);
    factory.registerAlias("OCTET_LENGTH", "length", FunctionFactory::Case::Insensitive);
}

}
