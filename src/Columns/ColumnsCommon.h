#pragma once

#include <Columns/IColumn.h>
#include <Common/PODArray.h>

#include <bit>

#if defined(__aarch64__) && defined(__ARM_NEON)
#    include <arm_neon.h>
#endif

/// Common helper methods for implementation of different columns.

namespace DB
{

[[noreturn]] void throwIndexesSizeTooSmall(size_t indexes_size, size_t limit);
[[noreturn]] void throwUnsupportedIndexesColumnType(const std::string & name);

/// Transform 64-byte mask to 64-bit mask
inline UInt64 bytes64MaskToBits64Mask(const UInt8 * bytes64)
{
#if defined(__aarch64__) && defined(__ARM_NEON)
    /// NEON has no instruction that extracts a bit per lane, so the mask is built by hand: each
    /// lane keeps its own bit of the result, and a pairwise-add tree folds the four groups into
    /// one register, leaving a single move out of the vector unit.
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
    return ~vgetq_lane_u64(vreinterpretq_u64_u8(sum0), 0);
#else
    if constexpr (std::endian::native == std::endian::little)
    {
        /// Compiles to `vpcmpeqb` plus `vpmovmskb` at `x86-64-v3`, and to a single `vptestmb`
        /// plus `kmovq` at `x86-64-v4`.
        using ByteVector = UInt8 __attribute__((ext_vector_type(64)));
        using BitMask = bool __attribute__((ext_vector_type(64)));
        static_assert(sizeof(BitMask) == sizeof(UInt64), "A lane of `BitMask` must be one bit");

        ByteVector bytes;
        __builtin_memcpy(&bytes, bytes64, sizeof(bytes));

        /// Converting to `bool` lanes is `!= 0`; a comparison would depend on `-faltivec-src-compat` on PowerPC.
        const BitMask mask = __builtin_convertvector(bytes, BitMask);

        UInt64 res;
        __builtin_memcpy(&res, &mask, sizeof(res));
        return res;
    }
    else
    {
        /// A bitcast of a vector of bits to an integer follows the endianness of the target, so on
        /// a big-endian machine the branch above puts the first byte in the most significant bit.
        UInt64 res = 0;
        for (size_t i = 0; i < 64; ++i)
            res |= static_cast<UInt64>(0 == bytes64[i]) << i;
        return ~res;
    }
#endif
}

/// Counts how many bytes of `filt` are greater than zero.
size_t countBytesInFilter(const UInt8 * filt, size_t start, size_t end);
size_t countBytesInFilter(const IColumn::Filter & filt);
size_t countBytesInFilterWithNull(const IColumn::Filter & filt, const UInt8 * null_map, size_t start, size_t end);

/// Returns vector with num_columns elements. vector[i] is the count of i values in selector.
/// Selector must contain values from 0 to num_columns - 1. NOTE: this is not checked.
VectorWithMemoryTracking<size_t> countColumnsSizeInSelector(size_t num_columns, const IColumn::Selector & selector);

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

/// In-place version of filterArraysImpl for when src and res are the same arrays
template <typename T>
void filterArraysImplInPlace(
    PaddedPODArray<T> & elems, IColumn::Offsets & offsets,
    const IColumn::Filter & filt);

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
        throwIndexesSizeTooSmall(indexes.size(), limit);

    if (const auto * data_uint8 = detail::getIndexesData<UInt8>(indexes))
        return column.template indexImpl<UInt8>(*data_uint8, limit);
    if (const auto * data_uint16 = detail::getIndexesData<UInt16>(indexes))
        return column.template indexImpl<UInt16>(*data_uint16, limit);
    if (const auto * data_uint32 = detail::getIndexesData<UInt32>(indexes))
        return column.template indexImpl<UInt32>(*data_uint32, limit);
    if (const auto * data_uint64 = detail::getIndexesData<UInt64>(indexes))
        return column.template indexImpl<UInt64>(*data_uint64, limit);

    throwUnsupportedIndexesColumnType(indexes.getName());
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

#define INSTANTIATE_INDEX_TEMPLATE_IMPL(ColumnTemplate) \
    template ColumnPtr ColumnTemplate<UInt8>::indexImpl<UInt8>(const PaddedPODArray<UInt8> & indexes, size_t limit) const; \
    template ColumnPtr ColumnTemplate<UInt16>::indexImpl<UInt16>(const PaddedPODArray<UInt16> & indexes, size_t limit) const; \
    template ColumnPtr ColumnTemplate<UInt32>::indexImpl<UInt32>(const PaddedPODArray<UInt32> & indexes, size_t limit) const; \
    template ColumnPtr ColumnTemplate<UInt64>::indexImpl<UInt64>(const PaddedPODArray<UInt64> & indexes, size_t limit) const;
}
