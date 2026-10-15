#pragma once

#include <base/Decimal.h>
#include <base/defines.h>
#include <base/types.h>

#include <algorithm>
#include <cstring>


namespace DB
{

template <typename T>
concept summable_into_decimal128 = is_decimal<T> && sizeof(T) <= sizeof(Decimal128);

namespace SumIntoDecimal128Impl
{

template <typename T, size_t size>
using Vector = T __attribute__((vector_size(size * sizeof(T))));

/// Eight 64-bit lanes: one AVX-512 register, two AVX2 registers or four SSE / NEON registers.
inline constexpr size_t lanes = 8;

using Int64Lanes = Vector<Int64, lanes>;
using UInt64Lanes = Vector<UInt64, lanes>;

/// Sets all bits of the lanes of the selected rows, i.e. those with `(flags[i] == 0) == add_if_zero`, and clears the others.
/// Every row takes `words_per_row` consecutive lanes.
/// An output parameter, because returning a vector wider than the target registers changes the ABI (`-Wpsabi`).
template <bool add_if_zero, size_t words_per_row>
ALWAYS_INLINE inline void loadRowMasks(const UInt8 * __restrict flags, UInt64Lanes & masks)
{
    static constexpr size_t rows = lanes / words_per_row;
    using RowMasks = Vector<Int64, rows>;

    Vector<UInt8, rows> row_flags;
    std::memcpy(&row_flags, flags, sizeof(row_flags));

    /// The flags are in [0, 255], so the sign bit of `flag - 1` is set iff `flag == 0`, and that of `-flag` iff `flag != 0`.
    /// Vector comparisons are avoided: their element type for bytes is `char`, which is unsigned on ARM and s390x,
    /// and they are deprecated for AltiVec vectors on PowerPC.
    const RowMasks wide_flags = __builtin_convertvector(row_flags, RowMasks);
    RowMasks row_masks;
    if constexpr (add_if_zero)
        row_masks = (wide_flags - 1) >> 63;
    else
        row_masks = (-wide_flags) >> 63;

    /// Duplicating the 64-bit masks rather than the flag bytes keeps the loads of the values unmasked:
    /// otherwise AVX-512 folds the mask into masked loads, which are much slower when reading from memory.
    if constexpr (words_per_row == 1)
        masks = __builtin_bit_cast(UInt64Lanes, row_masks);
    else
        masks = __builtin_bit_cast(UInt64Lanes, __builtin_shufflevector(row_masks, row_masks, 0, 0, 1, 1, 2, 2, 3, 3));
}

}

/// Sums `Decimal32`, `Decimal64` and `Decimal128` values into a `Decimal128`, wrapping modulo 2^128 like `Decimal128::addOverflow`.
///
/// `sum` and `avg` accumulate all these types in `Decimal128`, and adding to a `wide::integer<128>` is a scalar add-with-carry
/// chain. Here every value is split at bit 32 instead, and the parts are summed in 64-bit vector lanes (clang vector
/// extensions, so the same code is compiled to SSE, AVX2, AVX-512 or NEON). A 64-bit lane cannot overflow from adding
/// 32-bit parts within `max_rows_per_chunk` rows; the lanes are combined into a 128-bit value after each chunk.
///  - `Decimal32`: the values are sign-extended to 64 bits, and their sum fits into 64 bits.
///  - `Decimal64`: the lanes accumulate `s = sum(x) mod 2^64` and `a = sum(x >> 32)`. With `x = lower + (x >> 32) * 2^32`,
///    the true `sum(lower)` is below 2^64, so it equals `s - a * 2^32 mod 2^64`, and the exact sum is `sum(lower) + a * 2^32`.
///  - `Decimal128`: the column is read as 64-bit words, and every word `w` is added to `s += w` and `a += w >> 32` (logical).
///    For the low words, this is the same scheme with an unsigned split. The high words only matter modulo 2^64, so their
///    `s` is enough.
///
/// If `has_condition`, a row is added iff `(condition_map[i] == 0) == add_if_zero`. Rows are masked rather than skipped,
/// so the loops stay branch-free.
template <summable_into_decimal128 Value, bool has_condition = false, bool add_if_zero = false>
ALWAYS_INLINE inline Decimal128 sumIntoDecimal128(const Value * __restrict ptr, const UInt8 * __restrict condition_map, size_t count)
{
    using namespace SumIntoDecimal128Impl;

    static constexpr size_t max_rows_per_chunk = 1ULL << 32;
    static constexpr size_t words_per_row = sizeof(Value) == sizeof(Int128) ? 2 : 1;
    static constexpr size_t rows_per_step = lanes / words_per_row;

    auto row_mask = [&](size_t row) ALWAYS_INLINE -> UInt64
    {
        if constexpr (has_condition)
            return -static_cast<UInt64>((condition_map[row] == 0) == add_if_zero);
        else
            return ~0ULL;
    };

    Int128 result = 0;
    for (size_t chunk_begin = 0; chunk_begin < count; chunk_begin += max_rows_per_chunk)
    {
        const size_t chunk_end = std::min(count, chunk_begin + max_rows_per_chunk);
        UInt64Lanes s{};
        UInt64Lanes a{};

        size_t i = chunk_begin;
        for (; i + rows_per_step <= chunk_end; i += rows_per_step)
        {
            UInt64Lanes x;
            if constexpr (sizeof(Value) == sizeof(Int32))
            {
                Vector<Int32, lanes> values;
                std::memcpy(&values, ptr + i, sizeof(values));
                x = __builtin_bit_cast(UInt64Lanes, __builtin_convertvector(values, Int64Lanes));
            }
            else
                std::memcpy(&x, ptr + i, sizeof(x));

            if constexpr (has_condition)
            {
                UInt64Lanes masks;
                loadRowMasks<add_if_zero, words_per_row>(condition_map + i, masks);
                x &= masks;
            }

            s += x;
            if constexpr (sizeof(Value) == sizeof(Int64))
                a += __builtin_bit_cast(UInt64Lanes, __builtin_bit_cast(Int64Lanes, x) >> 32);
            else if constexpr (sizeof(Value) == sizeof(Int128))
                a += x >> 32;
        }

        if constexpr (sizeof(Value) == sizeof(Int32))
        {
            Int64 sum = static_cast<Int64>(__builtin_reduce_add(s));
            for (; i < chunk_end; ++i)
                sum += static_cast<Int64>(ptr[i].value) & static_cast<Int64>(row_mask(i));
            result += sum;
        }
        else if constexpr (sizeof(Value) == sizeof(Int64))
        {
            UInt64 sum = __builtin_reduce_add(s);
            Int64 sum_upper = static_cast<Int64>(__builtin_reduce_add(a));
            for (; i < chunk_end; ++i)
            {
                const Int64 x = ptr[i].value & static_cast<Int64>(row_mask(i));
                sum += static_cast<UInt64>(x);
                sum_upper += x >> 32;
            }
            const UInt64 sum_lower = sum - (static_cast<UInt64>(sum_upper) << 32);
            result += Int128(sum_lower) + (Int128(sum_upper) << 32);
        }
        else
        {
            /// The positions of the low and the high 64-bit word of a row, which depend on the endianness.
            static constexpr size_t low_word = Int128::_impl::little(0);
            static constexpr size_t high_word = Int128::_impl::little(1);

            UInt64 sum_low_words = __builtin_reduce_add(__builtin_shufflevector(s, s, low_word, low_word + 2, low_word + 4, low_word + 6));
            UInt64 sum_upper = __builtin_reduce_add(__builtin_shufflevector(a, a, low_word, low_word + 2, low_word + 4, low_word + 6));
            UInt64 sum_high_words = __builtin_reduce_add(__builtin_shufflevector(s, s, high_word, high_word + 2, high_word + 4, high_word + 6));
            for (; i < chunk_end; ++i)
            {
                const UInt64 mask = row_mask(i);
                const UInt64 w = ptr[i].value.items[low_word] & mask;
                sum_low_words += w;
                sum_upper += w >> 32;
                sum_high_words += ptr[i].value.items[high_word] & mask;
            }
            const UInt64 sum_lower = sum_low_words - (sum_upper << 32);
            result += Int128(sum_lower) + (Int128(sum_upper) << 32) + (Int128(sum_high_words) << 64);
        }
    }
    return Decimal128(result);
}

}
