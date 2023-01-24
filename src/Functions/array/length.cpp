#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Calculates the length of a string in bytes.
  */
struct LengthImpl
{
    static constexpr auto is_fixed_to_constant = true;

    static void vector(const ColumnString::Chars & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = offsets[i] - 1 - offsets[i - 1];
    }

    static void vectorFixedToConstant(const ColumnString::Chars & /*data*/, size_t n, UInt64 & res)
    {
        res = n;
    }

    static void vectorFixedToVector(const ColumnString::Chars & /*data*/, size_t /*n*/, PaddedPODArray<UInt64> & /*res*/)
    {
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = offsets[i] - offsets[i - 1];
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function length to UUID argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


struct NameLength
{
    static constexpr auto name = "length";
};

using FunctionLength = FunctionStringOrArrayToT<LengthImpl, NameLength, UInt64, false>;

REGISTER_FUNCTION(Length)
{
    factory.registerFunction<FunctionLength>(
        {
            R"(
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
            Documentation::Examples{
                {"string1", "SELECT length('Hello, world!')"},
                {"arr1", "SELECT length(['Hello'], ['world'])"},
                {"constexpr", "WITH 'hello' || toString(number) AS str\n"
                              "SELECT str, \n"
                              "       isConstant(length(str)) AS str_length_is_constant, \n"
                              "       isConstant(length(str::FixedString(6))) AS fixed_str_length_is_constant\n"
                              "FROM numbers(3)"},
                {"unicode", "SELECT 'ёлка' AS str1, length(str1), lengthUTF8(str1), normalizeUTF8NFKD(str1) AS str2, length(str2), lengthUTF8(str2)"},
                {"nul", R"(SELECT 'abc\0\0\0' AS str, length(str))"},
                },
            Documentation::Categories{"String", "Array"}
        },
        FunctionFactory::CaseInsensitive);
}

}
