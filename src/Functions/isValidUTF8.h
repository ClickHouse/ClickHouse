#pragma once

#include <Core/Types.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

struct ValidUTF8Impl
{
    static inline UInt8 isValidUTF8Naive(const UInt8 * data, UInt64 len);
    static inline UInt8 isValidUTF8(const UInt8 * data, UInt64 len);

    static constexpr bool is_fixed_to_constant = false;

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res);
    static void vectorFixedToConstant(const ColumnString::Chars & data, size_t n, UInt8 & res);
    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<UInt8> & res);

    [[noreturn]] static void array(const ColumnString::Offsets &, PaddedPODArray<UInt8> &);
    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t &, PaddedPODArray<UInt8> &);
};

}
