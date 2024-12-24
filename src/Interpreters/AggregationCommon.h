#pragma once

#include <array>

#include <Common/SipHash.h>
#include <Common/memcpySmall.h>
#include <Common/assert_cast.h>
#include <Core/Defines.h>
#include <base/StringRef.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>

#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
#include <tmmintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class Arena;

using Sizes = std::vector<size_t>;

/// When packing the values of nullable columns at a given row, we have to
/// store the fact that these values are nullable or not. This is achieved
/// by encoding this information as a bitmap. Let S be the size in bytes of
/// a packed values binary blob and T the number of bytes we may place into
/// this blob, the size that the bitmap shall occupy in the blob is equal to:
/// ceil(T/8). Thus we must have: S = T + ceil(T/8). Below we indicate for
/// each value of S, the corresponding value of T, and the bitmap size:
///
/// 32,28,4
/// 16,14,2
/// 8,7,1
/// 4,3,1
/// 2,1,1
///

template <typename T>
constexpr auto getBitmapSize()
{
    return
        (sizeof(T) == 32) ?
            4 :
        (sizeof(T) == 16) ?
            2 :
        ((sizeof(T) == 8) ?
            1 :
        ((sizeof(T) == 4) ?
            1 :
        ((sizeof(T) == 2) ?
            1 :
        0)));
}

template<typename T, size_t step>
void fillFixedBatch(size_t num_rows, const T * source, T * dest)
{
    for (size_t i = 0; i < num_rows; ++i)
    {
        *dest = *source;
        ++source;
        dest += step;
    }
}

/// Move keys of size T into binary blob, starting from offset.
/// It is assumed that offset is aligned to sizeof(T).
/// Example: sizeof(key) = 16, sizeof(T) = 4, offset = 8
/// out[0] : [--------****----]
/// out[1] : [--------****----]
/// ...
template<typename T, typename Key>
void fillFixedBatch(size_t keys_size, const ColumnRawPtrs & key_columns, const Sizes & key_sizes, PaddedPODArray<Key> & out, size_t & offset)
{
    for (size_t i = 0; i < keys_size; ++i)
    {
        if (key_sizes[i] == sizeof(T))
        {
            const auto * column = key_columns[i];
            size_t num_rows = column->size();
            out.resize_fill(num_rows);

            /// Note: here we violate strict aliasing.
            /// It should be ok as long as we do not refer to any value from `out` before filling.
            const char * source = static_cast<const ColumnFixedSizeHelper *>(column)->getRawDataBegin<sizeof(T)>();
            T * dest = reinterpret_cast<T *>(reinterpret_cast<char *>(out.data()) + offset);
            fillFixedBatch<T, sizeof(Key) / sizeof(T)>(num_rows, reinterpret_cast<const T *>(source), dest); /// NOLINT(bugprone-sizeof-expression)
            offset += sizeof(T);
        }
    }
}

/// Pack into a binary blob of type T a set of fixed-size keys. Granted that all the keys fit into the
/// binary blob. Keys are placed starting from the longest one.
template <typename T>
void packFixedBatch(size_t keys_size, const ColumnRawPtrs & key_columns, const Sizes & key_sizes, PaddedPODArray<T> & out)
{
    size_t offset = 0;
    fillFixedBatch<UInt128>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt64>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt32>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt16>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt8>(keys_size, key_columns, key_sizes, out, offset);
}

template <typename T>
using KeysNullMap = std::array<UInt8, getBitmapSize<T>()>;

/// Pack into a binary blob of type T a set of fixed-size keys. Granted that all the keys fit into the
/// binary blob, they are disposed in it consecutively.
template <typename T, bool has_low_cardinality = false>
static inline T ALWAYS_INLINE packFixed(
    size_t i, size_t keys_size, const ColumnRawPtrs & key_columns, const Sizes & key_sizes,
    const ColumnRawPtrs * low_cardinality_positions [[maybe_unused]] = nullptr,
    const Sizes * low_cardinality_sizes [[maybe_unused]] = nullptr)
{
    T key{};
    char * bytes = reinterpret_cast<char *>(&key);
    size_t offset = 0;

    for (size_t j = 0; j < keys_size; ++j)
    {
        size_t index = i;
        const IColumn * column = key_columns[j];
        if constexpr (has_low_cardinality)
        {
            if (const IColumn * positions = (*low_cardinality_positions)[j])
            {
                switch ((*low_cardinality_sizes)[j])
                {
                    case sizeof(UInt8): index = assert_cast<const ColumnUInt8 *>(positions)->getElement(i); break;
                    case sizeof(UInt16): index = assert_cast<const ColumnUInt16 *>(positions)->getElement(i); break;
                    case sizeof(UInt32): index = assert_cast<const ColumnUInt32 *>(positions)->getElement(i); break;
                    case sizeof(UInt64): index = assert_cast<const ColumnUInt64 *>(positions)->getElement(i); break;
                    default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for low cardinality column.");
                }
            }
        }

        switch (key_sizes[j])
        {
            case 1:
                {
                    memcpy(bytes + offset, static_cast<const ColumnFixedSizeHelper *>(column)->getRawDataBegin<1>() + index, 1);
                    offset += 1;
                }
                break;
            case 2:
                if constexpr (sizeof(T) >= 2)   /// To avoid warning about memcpy exceeding object size.
                {
                    memcpy(bytes + offset, static_cast<const ColumnFixedSizeHelper *>(column)->getRawDataBegin<2>() + index * 2, 2);
                    offset += 2;
                }
                break;
            case 4:
                if constexpr (sizeof(T) >= 4)
                {
                    memcpy(bytes + offset, static_cast<const ColumnFixedSizeHelper *>(column)->getRawDataBegin<4>() + index * 4, 4);
                    offset += 4;
                }
                break;
            case 8:
                if constexpr (sizeof(T) >= 8)
                {
                    memcpy(bytes + offset, static_cast<const ColumnFixedSizeHelper *>(column)->getRawDataBegin<8>() + index * 8, 8);
                    offset += 8;
                }
                break;
            default:
                memcpy(bytes + offset, static_cast<const ColumnFixedSizeHelper *>(column)->getRawDataBegin<1>() + index * key_sizes[j], key_sizes[j]);
                offset += key_sizes[j];
        }
    }

    return key;
}

/// Similar as above but supports nullable values.
template <typename T>
static inline T ALWAYS_INLINE packFixed(
    size_t i, size_t keys_size, const ColumnRawPtrs & key_columns, const Sizes & key_sizes,
    const KeysNullMap<T> & bitmap)
{
    union
    {
        T key;
        char bytes[sizeof(key)] = {};
    };

    size_t offset = 0;

    static constexpr auto bitmap_size = std::tuple_size<KeysNullMap<T>>::value;
    static constexpr bool has_bitmap = bitmap_size > 0;

    if (has_bitmap)
    {
        memcpy(bytes + offset, bitmap.data(), bitmap_size * sizeof(UInt8));
        offset += bitmap_size;
    }

    for (size_t j = 0; j < keys_size; ++j)
    {
        bool is_null;

        if (!has_bitmap)
            is_null = false;
        else
        {
            size_t bucket = j / 8;
            size_t off = j % 8;
            is_null = ((bitmap[bucket] >> off) & 1) == 1;
        }

        if (is_null)
            continue;

        switch (key_sizes[j])
        {
            case 1:
                memcpy(bytes + offset, static_cast<const ColumnFixedSizeHelper *>(key_columns[j])->getRawDataBegin<1>() + i, 1);
                offset += 1;
                break;
            case 2:
                memcpy(bytes + offset, static_cast<const ColumnFixedSizeHelper *>(key_columns[j])->getRawDataBegin<2>() + i * 2, 2);
                offset += 2;
                break;
            case 4:
                memcpy(bytes + offset, static_cast<const ColumnFixedSizeHelper *>(key_columns[j])->getRawDataBegin<4>() + i * 4, 4);
                offset += 4;
                break;
            case 8:
                memcpy(bytes + offset, static_cast<const ColumnFixedSizeHelper *>(key_columns[j])->getRawDataBegin<8>() + i * 8, 8);
                offset += 8;
                break;
            default:
                memcpy(bytes + offset, static_cast<const ColumnFixedSizeHelper *>(key_columns[j])->getRawDataBegin<1>() + i * key_sizes[j], key_sizes[j]);
                offset += key_sizes[j];
        }
    }

    return key;
}


/// Hash a set of keys into a UInt128 value.
static inline UInt128 ALWAYS_INLINE hash128( /// NOLINT
    size_t i, size_t keys_size, const ColumnRawPtrs & key_columns)
{
    SipHash hash;
    for (size_t j = 0; j < keys_size; ++j)
        key_columns[j]->updateHashWithValue(i, hash);

    return hash.get128();
}

/** Serialize keys into a continuous chunk of memory.
  */
static inline StringRef ALWAYS_INLINE serializeKeysToPoolContiguous( /// NOLINT
    size_t i, size_t keys_size, const ColumnRawPtrs & key_columns, Arena & pool)
{
    const char * begin = nullptr;

    size_t sum_size = 0;
    for (size_t j = 0; j < keys_size; ++j)
        sum_size += key_columns[j]->serializeValueIntoArena(i, pool, begin).size;

    return {begin, sum_size};
}


/** Pack elements with shuffle instruction.
  * See the explanation in ColumnsHashing.h
  */
#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
template <typename T>
static T inline packFixedShuffle(
    const char * __restrict * __restrict srcs,
    size_t num_srcs,
    const size_t * __restrict elem_sizes,
    size_t idx,
    const uint8_t * __restrict masks)
{
    assert(num_srcs > 0);

    __m128i res = _mm_shuffle_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(srcs[0] + elem_sizes[0] * idx)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(masks)));

    for (size_t i = 1; i < num_srcs; ++i)
    {
        res = _mm_xor_si128(res,
            _mm_shuffle_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(srcs[i] + elem_sizes[i] * idx)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&masks[i * sizeof(T)]))));
    }

    T out;
    __builtin_memcpy(&out, &res, sizeof(T));
    return out;
}
#endif

}
