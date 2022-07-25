#pragma once

#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#ifdef __SSE2__
#include <emmintrin.h>
#endif
#if defined(__AVX512F__) || defined(__AVX512BW__) || defined(__AVX__) || defined(__AVX2__)
#include <immintrin.h>
#endif
#if defined(__aarch64__) && defined(__ARM_NEON)
#    include <arm_neon.h>
#endif

/// Common helper methods for implementation of different columns.

namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

/// Transform 64-byte mask to 64-bit mask
inline UInt64 bytes64MaskToBits64Mask(const UInt8 * bytes64)
{
#if defined(__AVX512F__) && defined(__AVX512BW__)
    const __m512i zero64 = _mm512_setzero_epi32();
    UInt64 res = _mm512_cmp_epi8_mask(_mm512_loadu_si512(reinterpret_cast<const __m512i *>(bytes64)), zero64, _MM_CMPINT_EQ);
#elif defined(__AVX__) && defined(__AVX2__)
    const __m256i zero32 = _mm256_setzero_si256();
    UInt64 res =
        (static_cast<UInt64>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(bytes64)), zero32))) & 0xffffffff)
        | (static_cast<UInt64>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(bytes64+32)), zero32))) << 32);
#elif defined(__SSE2__) && defined(__POPCNT__)
    const __m128i zero16 = _mm_setzero_si128();
    UInt64 res =
        (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64)), zero16))) & 0xffff)
        | ((static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 16)), zero16))) << 16) & 0xffff0000)
        | ((static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 32)), zero16))) << 32) & 0xffff00000000)
        | ((static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 48)), zero16))) << 48) & 0xffff000000000000);
#elif defined(__aarch64__) && defined(__ARM_NEON)
    const uint8x16_t bitmask = {0x01, 0x02, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80, 0x01, 0x02, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};
    const auto * src = reinterpret_cast<const unsigned char *>(bytes64);
    const uint8x16_t p0 = vceqzq_u8(vld1q_u8(src));
    const uint8x16_t p1 = vceqzq_u8(vld1q_u8(src + 16));
    const uint8x16_t p2 = vceqzq_u8(vld1q_u8(src + 32));
    const uint8x16_t p3 = vceqzq_u8(vld1q_u8(src + 48));
    uint8x16_t t0 = vandq_u8(p0, bitmask);
    uint8x16_t t1 = vandq_u8(p1, bitmask);
    uint8x16_t t2 = vandq_u8(p2, bitmask);
    uint8x16_t t3 = vandq_u8(p3, bitmask);
    uint8x16_t sum0 = vpaddq_u8(t0, t1);
    uint8x16_t sum1 = vpaddq_u8(t2, t3);
    sum0 = vpaddq_u8(sum0, sum1);
    sum0 = vpaddq_u8(sum0, sum0);
    UInt64 res = vgetq_lane_u64(vreinterpretq_u64_u8(sum0), 0);
#else
    UInt64 res = 0;
    for (size_t i = 0; i < 64; ++i)
        res |= static_cast<UInt64>(0 == bytes64[i]) << i;
#endif
    return ~res;
}

/// Counts how many bytes of `filt` are greater than zero.
size_t countBytesInFilter(const UInt8 * filt, size_t start, size_t end);
size_t countBytesInFilter(const IColumn::Filter & filt);
size_t countBytesInFilterWithNull(const IColumn::Filter & filt, const UInt8 * null_map, size_t start, size_t end);

/// Returns vector with num_columns elements. vector[i] is the count of i values in selector.
/// Selector must contain values from 0 to num_columns - 1. NOTE: this is not checked.
std::vector<size_t> countColumnsSizeInSelector(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector);

/// Returns true, if the memory contains only zeros.
bool memoryIsZero(const void * data, size_t start, size_t end);
bool memoryIsByte(const void * data, size_t start, size_t end, uint8_t byte);

/// The general implementation of `filter` function for ColumnArray and ColumnString.
template <typename T>
void filterArraysImpl(
    const PaddedPODArray<T> & src_elems, const IColumn::Offsets & src_offsets,
    PaddedPODArray<T> & res_elems, IColumn::Offsets & res_offsets,
    const IColumn::Filter & filt, ssize_t result_size_hint);

/// Same as above, but not fills res_offsets.
template <typename T>
void filterArraysImplOnlyData(
    const PaddedPODArray<T> & src_elems, const IColumn::Offsets & src_offsets,
    PaddedPODArray<T> & res_elems,
    const IColumn::Filter & filt, ssize_t result_size_hint);

namespace detail
{
    template <typename T>
    const PaddedPODArray<T> * getIndexesData(const IColumn & indexes);
}

/// Check limit <= indexes->size() and call column.indexImpl(const PaddedPodArray<Type> & indexes, UInt64 limit).
template <typename Column>
ColumnPtr selectIndexImpl(const Column & column, const IColumn & indexes, size_t limit)
{
    if (limit == 0)
        limit = indexes.size();

    if (indexes.size() < limit)
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Size of indexes ({}) is less than required ({})", indexes.size(), limit);

    if (const auto * data_uint8 = detail::getIndexesData<UInt8>(indexes))
        return column.template indexImpl<UInt8>(*data_uint8, limit);
    else if (const auto * data_uint16 = detail::getIndexesData<UInt16>(indexes))
        return column.template indexImpl<UInt16>(*data_uint16, limit);
    else if (const auto * data_uint32 = detail::getIndexesData<UInt32>(indexes))
        return column.template indexImpl<UInt32>(*data_uint32, limit);
    else if (const auto * data_uint64 = detail::getIndexesData<UInt64>(indexes))
        return column.template indexImpl<UInt64>(*data_uint64, limit);
    else
        throw Exception("Indexes column for IColumn::select must be ColumnUInt, got " + indexes.getName(),
                        ErrorCodes::LOGICAL_ERROR);
}

size_t getLimitForPermutation(size_t column_size, size_t perm_size, size_t limit);

template <typename Column>
ColumnPtr permuteImpl(const Column & column, const IColumn::Permutation & perm, size_t limit)
{
    limit = getLimitForPermutation(column.size(), perm.size(), limit);
    return column.indexImpl(perm, limit);
}

/// NOLINTNEXTLINE
#define INSTANTIATE_INDEX_IMPL(Column) \
    template ColumnPtr Column::indexImpl<UInt8>(const PaddedPODArray<UInt8> & indexes, size_t limit) const; \
    template ColumnPtr Column::indexImpl<UInt16>(const PaddedPODArray<UInt16> & indexes, size_t limit) const; \
    template ColumnPtr Column::indexImpl<UInt32>(const PaddedPODArray<UInt32> & indexes, size_t limit) const; \
    template ColumnPtr Column::indexImpl<UInt64>(const PaddedPODArray<UInt64> & indexes, size_t limit) const;
}
