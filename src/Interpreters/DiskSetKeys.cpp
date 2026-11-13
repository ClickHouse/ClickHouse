#include <Interpreters/DiskSetKeys.h>

#include <Columns/ColumnsCommon.h>
#include <Common/RadixSort.h>
#include <base/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Calls `f` with the function that maps a value of a fixed-size key column, at a pointer to its `size`
/// bytes, to an unsigned integer in a way that keeps the order, so that a smaller value gets a smaller
/// integer. `encodeFixedSizeKeys` builds disk keys from these integers.
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

template <typename Key>
ColumnPtr encodeFixedSizeKeys(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, ConstNullMapPtr null_map)
{
    ColumnRawPtrs value_columns(key_columns.size());
    std::vector<const NullMap *> null_maps(key_columns.size());
    size_t key_bits = 0;
    for (size_t i = 0; i < key_columns.size(); ++i)
    {
        value_columns[i] = key_columns[i];
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(key_columns[i]))
        {
            value_columns[i] = &nullable->getNestedColumn();
            null_maps[i] = &nullable->getNullMapData();
            ++key_bits;
        }
        key_bits += 8 * key_sizes[i];
    }
    /// The disk keys fit `Key`, because the method of the set packs the same values into it, and a nullable
    /// method also a bitmap with a bit for each column.
    chassert(key_bits <= 8 * sizeof(Key));

    const size_t num_keys = null_map ? rows - countBytesInFilter(*null_map) : rows;
    auto keys = ColumnVector<Key>::create(num_keys, Key(0));
    auto & data = keys->getData();

    /// Adds `get_part(row)`, which takes `bits` bits, to each key below the parts added before.
    size_t shift = key_bits;
    auto add_part = [&](size_t bits, auto && get_part)
    {
        shift -= bits;
        size_t key = 0;
        for (size_t row = 0; row < rows; ++row)
        {
            if (null_map && (*null_map)[row])
                continue;
            data[key] |= static_cast<Key>(get_part(row) << shift);
            ++key;
        }
    };

    for (size_t i = 0; i < value_columns.size(); ++i)
    {
        const char * column_data = value_columns[i]->getRawData().data();
        const size_t size = key_sizes[i];
        const NullMap * column_null_map = null_maps[i];

        /// With `transform_null_in`, `NULL` is a value that sorts after the other values of its column, as in
        /// `ORDER BY`: a nullable column starts with a bit that is set for `NULL`, whose value bits are zero.
        if (column_null_map)
            add_part(1, [&](size_t row) { return Key((*column_null_map)[row]); });

        callWithOrderedBits<Key>(*value_columns[i], size, [&](auto get_bits)
        {
            add_part(8 * size, [&](size_t row)
            {
                if (column_null_map && (*column_null_map)[row])
                    return Key(0);
                return static_cast<Key>(get_bits(column_data + row * size));
            });
        });
    }

    return keys;
}

template ColumnPtr encodeFixedSizeKeys<UInt8>(const ColumnRawPtrs &, const Sizes &, size_t, ConstNullMapPtr);
template ColumnPtr encodeFixedSizeKeys<UInt16>(const ColumnRawPtrs &, const Sizes &, size_t, ConstNullMapPtr);
template ColumnPtr encodeFixedSizeKeys<UInt32>(const ColumnRawPtrs &, const Sizes &, size_t, ConstNullMapPtr);
template ColumnPtr encodeFixedSizeKeys<UInt64>(const ColumnRawPtrs &, const Sizes &, size_t, ConstNullMapPtr);
template ColumnPtr encodeFixedSizeKeys<UInt128>(const ColumnRawPtrs &, const Sizes &, size_t, ConstNullMapPtr);
template ColumnPtr encodeFixedSizeKeys<UInt256>(const ColumnRawPtrs &, const Sizes &, size_t, ConstNullMapPtr);

ColumnPtr computePrefixedHashes(const IColumn & column, size_t rows, ConstNullMapPtr null_map)
{
    auto keys = ColumnVector<UInt128>::create();
    keys->reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        if (null_map && (*null_map)[row])
            continue;
        keys->getData().push_back(getPrefixedHash(column.getDataAt(row)));
    }
    return keys;
}

ColumnPtr computeHashedKeys(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, ConstNullMapPtr null_map)
{
    using HashedMethod = decltype(SetVariants::hashed)::element_type;
    HashedMethod::State state(key_columns, key_sizes, nullptr);
    Arena pool;
    auto keys = ColumnVector<HashedMethod::Key>::create();
    keys->reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        if (null_map && (*null_map)[row])
            continue;
        auto key_holder = state.getKeyHolder(row, pool);
        keys->getData().push_back(keyHolderGetKey(key_holder));
    }
    return keys;
}

}
