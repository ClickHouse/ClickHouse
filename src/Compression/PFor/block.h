#pragma once

// One block of up to BLOCK residuals: frame-of-reference bit-packing with patched exceptions (PForDelta).
// Layout:
//   byte0 high bit set  -> constant block: low 7 bits = k value bytes, then k LE bytes (k == 0 => constant 0).
//   byte0 high bit clear -> normal block:
//        byte0 = base_width      base bit width (0..typeBits<T>)
//        byte1 = num_exceptions  values needing more than base_width bits
//        byte2 = patch_width     (only if num_exceptions > 0) high-bit width of the patches
//        base                    cnt values, low base_width bits each, bit-packed
//        positions               num_exceptions bytes, the exception indices: strictly increasing, each < cnt
//        patches                 num_exceptions values, patch_width bits each; patch = value >> base_width
// Decode unpacks the base then ORs each exception's high bits back in; base_width minimises total bytes.
// A full 128-value uint32 block with base_width in [1,31] uses the SIMD vertical layout, else the scalar packer (same packedBytes, so the stream is identical).

#include <Compression/PFor/bitpack.h>
#include <Compression/PFor/common.h>
#include <Compression/PFor/vertical.h>

#include <algorithm>
#include <bit>
#include <cstring>

namespace DB::PFor::detail
{

template <typename T>
inline ALWAYS_INLINE void packBase(const T * r, unsigned cnt, unsigned b, uint8_t * p) noexcept
{
#if PFOR_HAS_VERTICAL
    if constexpr (sizeof(T) == 4)
        if (cnt == BLOCK && b >= 1 && b <= 31)
        {
            packVertical32(reinterpret_cast<const uint32_t *>(r), b, p);
            return;
        }
#endif
    packBits<T>(r, cnt, b, p);
}

template <typename T>
inline ALWAYS_INLINE void unpackBase(const uint8_t * p, unsigned cnt, unsigned b, T * out) noexcept
{
#if PFOR_HAS_VERTICAL
    if constexpr (sizeof(T) == 4)
        if (cnt == BLOCK && b >= 1 && b <= 31)
        {
            unpackVertical32(p, b, reinterpret_cast<uint32_t *>(out));
            return;
        }
#endif
    unpackBits<T>(p, cnt, b, out);
}

/// The base width of a normal block, its number of exceptions, and the width of its widest value.
struct BaseWidthChoice
{
    unsigned max_width;
    unsigned base_width;
    unsigned num_exceptions;
};

/// Picks the base width that minimises the block. `count_wider_than(width)` is the number of values needing more than
/// `width` bits, the exceptions of base width `width`; it is called with decreasing widths. The exception count of the
/// winning width is recorded here rather than recounted over the block.
template <typename CountWiderThan>
inline ALWAYS_INLINE BaseWidthChoice chooseBaseWidth(unsigned cnt, unsigned max_width, CountWiderThan && count_wider_than) noexcept
{
    size_t best_cost = 2 + packedBytes(cnt, max_width);
    unsigned best_width = max_width;
    unsigned best_num_exceptions = 0;
    for (int width = static_cast<int>(max_width) - 1; width >= 0; --width)
    {
        const unsigned num_exceptions_at_width = count_wider_than(static_cast<unsigned>(width));
        const unsigned patch_width = max_width - static_cast<unsigned>(width);
        const size_t cost = 3 + packedBytes(cnt, static_cast<unsigned>(width))
            + num_exceptions_at_width + packedBytes(num_exceptions_at_width, patch_width);
        if (cost < best_cost)
        {
            best_cost = cost;
            best_width = static_cast<unsigned>(width);
            best_num_exceptions = num_exceptions_at_width;
        }
    }
    return {max_width, best_width, best_num_exceptions};
}

/// Chooses the base width with a bit-width histogram; `value_width(i)` is the bit width of value `i`.
template <typename T, typename ValueWidth>
inline ALWAYS_INLINE BaseWidthChoice chooseBaseWidthWithHistogram(unsigned cnt, ValueWidth && value_width) noexcept
{
    unsigned histogram[typeBits<T> + 1] = {0};
    unsigned max_width = 0;
    for (unsigned i = 0; i < cnt; ++i)
    {
        const unsigned width = value_width(i);
        ++histogram[width];
        max_width = std::max(width, max_width);
    }

    // The count of values needing more than `width` bits is a suffix sum over the histogram.
    unsigned num_exceptions = 0;
    return chooseBaseWidth(cnt, max_width, [&](unsigned width)
    {
        num_exceptions += histogram[width + 1];
        return num_exceptions;
    });
}

/// The SIMD exception count compares vectors, which AltiVec types as a deprecated `vector bool`, so PowerPC uses the histogram.
#if PFOR_HAS_VERTICAL && !defined(__ALTIVEC__)
#    define PFOR_HAS_SIMD_EXCEPTION_COUNT 1
#else
#    define PFOR_HAS_SIMD_EXCEPTION_COUNT 0
#endif

#if PFOR_HAS_SIMD_EXCEPTION_COUNT
/// Chooses the base width by counting the exceptions of every candidate width with SIMD over the values' bit widths,
/// zero-padded to `padded_cnt`: a zero width is never an exception.
inline ALWAYS_INLINE BaseWidthChoice chooseBaseWidthWithSimd(const uint8_t * widths, unsigned cnt, unsigned padded_cnt, unsigned max_width) noexcept
{
    using v16u8 = uint8_t __attribute__((vector_size(16)));
    return chooseBaseWidth(cnt, max_width, [&](unsigned width)
    {
        const auto w = static_cast<uint8_t>(width);
        const v16u8 width_vector = {w, w, w, w, w, w, w, w, w, w, w, w, w, w, w, w};
        // A comparison yields -1 in every true lane, so subtracting it counts. A lane counts at most BLOCK / 16.
        decltype(width_vector > width_vector) counts = {};
        for (unsigned i = 0; i < padded_cnt; i += 16)
        {
            v16u8 chunk;
            std::memcpy(&chunk, widths + i, 16);
            counts -= chunk > width_vector;
        }
        unsigned total = 0;
        for (unsigned lane = 0; lane < 16; ++lane)
            total += static_cast<unsigned>(counts[lane]);
        return total;
    });
}
#endif

/// The SIMD count costs a step per 16-value chunk and a reduction for every candidate width, while the increments of
/// a histogram serialise on the counter most values share. So 32-bit blocks of at least one chunk use the SIMD count
/// while they have at most 16 candidate widths per chunk, and other blocks and 64-bit values use the histogram.
template <typename T>
inline ALWAYS_INLINE BaseWidthChoice chooseBlockBaseWidth(const T * residuals, unsigned cnt) noexcept
{
#if PFOR_HAS_SIMD_EXCEPTION_COUNT
    if constexpr (sizeof(T) == 4)
    {
        if (cnt >= 16)
        {
            alignas(16) uint8_t widths[BLOCK];
            T all_bits = 0;
            for (unsigned i = 0; i < cnt; ++i)
            {
                widths[i] = static_cast<uint8_t>(std::bit_width(residuals[i]));
                all_bits |= residuals[i];
            }
            const unsigned padded_cnt = (cnt + 15) / 16 * 16;
            std::fill(widths + cnt, widths + padded_cnt, uint8_t{0});
            const auto max_width = static_cast<unsigned>(std::bit_width(all_bits));
            if (max_width <= padded_cnt)
                return chooseBaseWidthWithSimd(widths, cnt, padded_cnt, max_width);
            return chooseBaseWidthWithHistogram<T>(cnt, [&](unsigned i) -> unsigned { return widths[i]; });
        }
    }
#endif
    return chooseBaseWidthWithHistogram<T>(cnt, [&](unsigned i) { return bitWidth(residuals[i]); });
}

/// Encodes a block whose values are not all equal. Not inlined into `blockEncode`, so that its larger stack frame does
/// not slow down constant blocks.
template <typename T>
NO_INLINE size_t normalBlockEncode(const T * residuals, unsigned cnt, uint8_t * out) noexcept
{
    const BaseWidthChoice choice = chooseBlockBaseWidth(residuals, cnt);
    const unsigned max_width = choice.max_width;
    const unsigned base_width = choice.base_width;
    const unsigned num_exceptions = choice.num_exceptions;

    out[0] = static_cast<uint8_t>(base_width);
    out[1] = static_cast<uint8_t>(num_exceptions);
    uint8_t * p = out + 2;
    unsigned patch_width = 0;
    if (num_exceptions)
    {
        patch_width = max_width - base_width;
        *p++ = static_cast<uint8_t>(patch_width);
    }

    packBase<T>(residuals, cnt, base_width, p);
    p += packedBytes(cnt, base_width);

    if (num_exceptions)
    {
        uint8_t * exception_positions = p;
        p += num_exceptions;
        T patches[BLOCK];
        unsigned num_patches = 0;
        // base_width < max_width here, so the shift is defined (a full-width block has no exceptions).
        for (unsigned i = 0; i < cnt; ++i)
        {
            const T high_bits = static_cast<T>(residuals[i] >> base_width);
            if (high_bits)
            {
                exception_positions[num_patches] = static_cast<uint8_t>(i);
                patches[num_patches] = high_bits;
                ++num_patches;
            }
        }
        packBits<T>(patches, num_exceptions, patch_width, p);
        p += packedBytes(num_exceptions, patch_width);
    }
    return static_cast<size_t>(p - out);
}

template <typename T>
inline size_t blockEncode(const T * residuals, unsigned cnt, uint8_t * out) noexcept
{
    const bool all_equal = std::ranges::all_of(residuals + 1, residuals + cnt, [first = residuals[0]](T value) { return value == first; });
    if (all_equal)
    {
        const T constant = residuals[0];
        const unsigned num_bytes = (bitWidth(constant) + 7u) / 8u; // 0..sizeof(T)
        out[0] = static_cast<uint8_t>(0x80u | num_bytes);
        storeLE(out + 1, static_cast<uint64_t>(constant), num_bytes);
        return 1u + num_bytes;
    }
    return normalBlockEncode(residuals, cnt, out);
}

// Reconstruct values from residuals in `out` (prefix sum + carry). SIMD for uint32, scalar for uint64; `plus` is 0 for d0, 1 for d1.
template <typename T, uint32_t plus>
inline ALWAYS_INLINE void deltaApply(T * out, unsigned cnt, T & prev) noexcept
{
#if PFOR_HAS_VERTICAL
    if constexpr (sizeof(T) == 4)
    {
        uint32_t carry = static_cast<uint32_t>(prev);
        deltaDecode32<plus>(reinterpret_cast<uint32_t *>(out), cnt, carry);
        prev = static_cast<T>(carry);
        return;
    }
#endif
    T acc = prev;
    for (unsigned i = 0; i < cnt; ++i)
    {
        acc = static_cast<T>(acc + out[i] + plus);
        out[i] = acc;
    }
    prev = acc;
}

// Decodes one block. With non-null `end` it is fail-closed: reads are bounded and every header field validated; returns 0 on any violation (a valid block is >= 1 byte) so the caller can report corruption. nullptr keeps the fast path (field validation still runs).
template <typename T>
inline size_t blockDecode(const uint8_t * in, unsigned cnt, T * out, Delta mode, T & prev, const uint8_t * end = nullptr) noexcept
{
    const auto need = [end](const uint8_t * from, size_t bytes) noexcept
    {
        return !end || (from <= end && static_cast<size_t>(end - from) >= bytes);
    };

    if (!need(in, 1))
        return 0;
    const uint8_t b0 = in[0];
    if (b0 & 0x80u)
    {
        const unsigned k = b0 & 0x7Fu;
        if (k > sizeof(T) || !need(in + 1, k)) // loadLE reads k bytes
            return 0;
        const T c = static_cast<T>(loadLE(in + 1, k));
        for (unsigned i = 0; i < cnt; ++i)
            out[i] = c;
        if (mode == Delta::d1)
            deltaApply<T, 1>(out, cnt, prev);
        else if (mode == Delta::d0)
            deltaApply<T, 0>(out, cnt, prev);
        return 1u + k;
    }

    const unsigned b = b0;
    if (b > typeBits<T> || !need(in, 2))
        return 0;
    const unsigned e = in[1];
    if (e > cnt || (e && b >= typeBits<T>)) // e values must fit patches[BLOCK]; exceptions need b < typeBits (shift-safe)
        return 0;
    const uint8_t * p = in + 2;
    unsigned hb = 0;
    if (e)
    {
        if (!need(p, 1))
            return 0;
        hb = *p++;
        // Valid exceptions have hb in [1, typeBits<T> - b]; hb == 0 drops the patch, larger shifts bits out of T (b < typeBits<T> here, so no underflow).
        if (hb == 0 || hb > typeBits<T> - b)
            return 0;
    }

    const size_t base_bytes = packedBytes(cnt, b);
    if (!need(p, base_bytes))
        return 0;

    // Fused single pass: a full uint32 delta block with no exceptions unpacks and prefix-sums in one sweep.
#if PFOR_HAS_VERTICAL
    if constexpr (sizeof(T) == 4)
        if (mode != Delta::none && e == 0 && cnt == BLOCK && b >= 1 && b <= 31)
        {
            uint32_t carry = static_cast<uint32_t>(prev);
            if (mode == Delta::d1)
                unpackVertical32FusedDelta<1>(p, b, reinterpret_cast<uint32_t *>(out), carry);
            else
                unpackVertical32FusedDelta<0>(p, b, reinterpret_cast<uint32_t *>(out), carry);
            prev = static_cast<T>(carry);
            return static_cast<size_t>((p + base_bytes) - in);
        }
#endif

    unpackBase<T>(p, cnt, b, out);
    p += base_bytes;

    if (e)
    {
        if (!need(p, e))
            return 0;
        const uint8_t * pos = p;
        p += e;
        const size_t patch_bytes = packedBytes(e, hb);
        if (!need(p, patch_bytes))
            return 0;
        T patches[BLOCK];
        unpackBits<T>(p, e, hb, patches);
        p += patch_bytes;
        for (unsigned j = 0; j < e; ++j)
        {
            // blockEncode emits strictly increasing positions; a duplicate would OR two patches into one value.
            if ((pos[j] >= cnt) || ((j > 0) && (pos[j] <= pos[j - 1])))
                return 0;
            out[pos[j]] |= patches[j] << b;
        }
    }
    if (mode == Delta::d1)
        deltaApply<T, 1>(out, cnt, prev);
    else if (mode == Delta::d0)
        deltaApply<T, 0>(out, cnt, prev);
    return static_cast<size_t>(p - in);
}

}
