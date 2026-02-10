#pragma once

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

    static void vector(const ColumnString::Chars & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res, size_t input_rows_count)
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            res[i] = offsets[i] - 1 - offsets[i - 1];
    }

    static void vectorFixedToConstant(const ColumnString::Chars & /*data*/, size_t n, UInt64 & res, size_t)
    {
        res = n;
    }

    static void vectorFixedToVector(const ColumnString::Chars & /*data*/, size_t /*n*/, PaddedPODArray<UInt64> & /*res*/, size_t)
    {
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res, size_t input_rows_count)
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            res[i] = offsets[i] - offsets[i - 1];
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t &, PaddedPODArray<UInt64> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function length to UUID argument");
    }

    [[noreturn]] static void ipv6(const ColumnIPv6::Container &, size_t &, PaddedPODArray<UInt64> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function length to IPv6 argument");
    }

    [[noreturn]] static void ipv4(const ColumnIPv4::Container &, size_t &, PaddedPODArray<UInt64> &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot apply function length to IPv4 argument");
    }
};


struct NameLength
{
    static constexpr auto name = "length";
};

using FunctionLength = FunctionStringOrArrayToT<LengthImpl, NameLength, UInt64, false>;

}
