#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <array>

#if defined(__SSSE3__) || (defined(__aarch64__) && defined(__ARM_NEON))
namespace DB
{

#if defined(__AVX2__)
using UInt32x8 = UInt32 __attribute__((vector_size(32)));
using UInt8x8 = UInt8 __attribute__((vector_size(8)));

/// Returns `source[control[i]]` in lane `i`. Clang lowers this loop to a single `vpermd`.
ALWAYS_INLINE inline UInt32x8 permuteDwords(UInt32x8 source, UInt32x8 control)
{
    UInt32x8 res{};
    for (size_t i = 0; i < 8; ++i)
        res[i] = source[control[i]];
    return res;
}

/// 4 and 8-byte elements are permuted as dwords in a 32-byte vector (`vpermd`), which moves twice as many rows per lookup.
/// It needs AVX2: without it the 32-byte permute is split or scalarized and is slower than two 16-byte shuffles.
/// NEON has no such permute, so ARM keeps the 16-byte shuffle.
template <size_t ELEMENT_WIDTH>
constexpr bool COMPRESS_DWORDS = ELEMENT_WIDTH >= 4;
#else
template <size_t ELEMENT_WIDTH>
constexpr bool COMPRESS_DWORDS = false;
#endif

/// One shuffle handles 8 rows of 1-byte elements (a 256-entry table of 16 bytes would be too large for 16 rows),
/// otherwise the rows of a 16-byte vector (32-byte for `COMPRESS_DWORDS`).
template <size_t ELEMENT_WIDTH>
constexpr size_t COMPRESS_ROWS = ELEMENT_WIDTH == 1 ? 8 : (COMPRESS_DWORDS<ELEMENT_WIDTH> ? 32 : 16) / ELEMENT_WIDTH;

/// For each mask of `COMPRESS_ROWS` rows, the control that moves the selected elements to the front: byte indices,
/// or dword indices for `COMPRESS_DWORDS`.
template <size_t ELEMENT_WIDTH>
alignas(16) constexpr auto compress_table = []
{
    constexpr size_t rows = COMPRESS_ROWS<ELEMENT_WIDTH>;
    constexpr size_t units = COMPRESS_DWORDS<ELEMENT_WIDTH> ? ELEMENT_WIDTH / 4 : ELEMENT_WIDTH;
    std::array<std::array<UInt8, 16>, 1 << rows> table{};
    for (size_t mask = 0; mask < table.size(); ++mask)
    {
        size_t out = 0;
        for (size_t row = 0; row < rows; ++row)
        {
            if ((mask >> row) & 1)
            {
                for (size_t unit = 0; unit < units; ++unit)
                    table[mask][out++] = static_cast<UInt8>(row * units + unit);
            }
        }
    }
    return table;
}();

}
#endif
