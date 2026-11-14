#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>
#include <Common/RadixSort.h>
#include <base/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Calls `f` with the function that maps a value of a fixed-size key column, at a pointer to its `size`
/// bytes, to an unsigned integer in a way that keeps the order, so that a smaller value gets a smaller
/// integer. Disk keys of sets and packed sort keys of blocks are built from these integers.
template <typename Key, typename F>
void callWithOrderedBits(const IColumn & column, size_t size, F && f)
{
    auto call_with_number_bits = [&]<template <typename> typename Transform>()
    {
        auto call = [&]<typename T>()
        {
            f([](const char * value) { return Transform<T>::forward(unalignedLoad<T>(value)); });
        };

        switch (size)
        {
            case 1: return call.template operator()<UInt8>();
            case 2: return call.template operator()<UInt16>();
            case 4: return call.template operator()<UInt32>();
            case 8: return call.template operator()<UInt64>();
            case 16: return call.template operator()<UInt128>();
            case 32: return call.template operator()<UInt256>();
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size {} of a number in a key of a set on disk", size);
        }
    };

    const TypeIndex type = column.getDataType();
    /// `IPv6` sorts by the bytes of the address, as `FixedString` does, not by its integer value.
    if (isFixedString(type) || isIPv6(type))
        f([size](const char * value) { return unalignedLoadBigEndian<Key>(value, size); });
    else if (isFloat(type))
        call_with_number_bits.template operator()<RadixSortFloatTransform>();
    /// Signed integers, and decimals, whose values are signed integers.
    else if (isInt(type) || isColumnedAsDecimal(type))
        call_with_number_bits.template operator()<RadixSortSignedTransform>();
    /// Unsigned integers, such as `UInt32`, `DateTime` and `UUID`.
    else
        call_with_number_bits.template operator()<RadixSortIdentityTransform>();
}

}
