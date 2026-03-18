#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>
#include <Common/UTF8Helpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** If the string is UTF-8 encoded text, it returns the length of the text in code points.
  * (not in characters: the length of the text "ё" can be either 1 or 2, depending on the normalization)
 * (not in characters: the length of the text "" can be either 1 or 2, depending on the normalization)
  * Otherwise, the behavior is undefined.
  */
struct LengthUTF8Impl
{
    static constexpr auto is_fixed_to_constant = false;

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res, size_t input_rows_count)
    {
        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = UTF8::countCodePoints(&data[prev_offset], offsets[i] - prev_offset - 1);
            prev_offset = offsets[i];
        }
    }

    static void vectorFixedToConstant(const ColumnString::Chars & /*data*/, size_t /*n*/, UInt64 & /*res*/, size_t)
    {
    }

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<UInt64> & res, size_t input_rows_count)
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            res[i] = UTF8::countCodePoints(&data[i * n], n);
    }

    [[noreturn]] static void array(const ColumnString::Offsets &, PaddedPODArray<UInt64> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function lengthUTF8 to Array argument");
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t &, PaddedPODArray<UInt64> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function lengthUTF8 to UUID argument");
    }

    [[noreturn]] static void ipv6(const ColumnIPv6::Container &, size_t &, PaddedPODArray<UInt64> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function lengthUTF8 to IPv6 argument");
    }

    [[noreturn]] static void ipv4(const ColumnIPv4::Container &, size_t &, PaddedPODArray<UInt64> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function lengthUTF8 to IPv4 argument");
    }
};

struct NameLengthUTF8
{
    static constexpr auto name = "lengthUTF8";
};
using FunctionLengthUTF8 = FunctionStringOrArrayToT<LengthUTF8Impl, NameLengthUTF8, UInt64>;

}

REGISTER_FUNCTION(LengthUTF8)
{
    factory.registerFunction<FunctionLengthUTF8>();

    /// Compatibility aliases.
    factory.registerAlias("CHAR_LENGTH", "lengthUTF8", FunctionFactory::Case::Insensitive);
    factory.registerAlias("CHARACTER_LENGTH", "lengthUTF8", FunctionFactory::Case::Insensitive);
}

}
