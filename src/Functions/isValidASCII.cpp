#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>
#include <Common/isValidASCII.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct ValidASCIIImpl
{
    static UInt8 isValidASCII(const UInt8 * data, UInt64 len) { return DB::ASCII::isValidASCII(data, len); }

    static constexpr bool is_fixed_to_constant = false;

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res, size_t input_rows_count)
    {
        size_t prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = isValidASCII(data.data() + prev_offset, offsets[i] - 1 - prev_offset);
            prev_offset = offsets[i];
        }
    }

    static void vectorFixedToConstant(const ColumnString::Chars &, size_t, UInt8 &, size_t)
    {
    }

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<UInt8> & res, size_t input_rows_count)
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            res[i] = isValidASCII(data.data() + i * n, n);
    }

    [[noreturn]] static void array(const ColumnString::Offsets &, PaddedPODArray<UInt8> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function isValidASCII to Array argument");
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t &, PaddedPODArray<UInt8> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function isValidASCII to UUID argument");
    }

    [[noreturn]] static void ipv6(const ColumnIPv6::Container &, size_t &, PaddedPODArray<UInt8> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function isValidASCII to IPv6 argument");
    }

    [[noreturn]] static void ipv4(const ColumnIPv4::Container &, size_t &, PaddedPODArray<UInt8> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function isValidASCII to IPv4 argument");
    }
};

struct NameIsValidASCII
{
    static constexpr auto name = "isValidASCII";
};
using FunctionValidASCII = FunctionStringOrArrayToT<ValidASCIIImpl, NameIsValidASCII, UInt8>;

REGISTER_FUNCTION(IsValidASCII)
{
    factory.registerFunction<FunctionValidASCII>();
    factory.registerAlias("isASCII", "isValidASCII", FunctionFactory::Case::Insensitive);
}

}
