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

template <typename T>
inline size_t blockEncode(const T * residuals, unsigned cnt, uint8_t * out) noexcept
{
    bool all_equal = true;
    for (unsigned i = 1; i < cnt; ++i)
        if (residuals[i] != residuals[0])
        {
            all_equal = false;
            break;
        }
    if (all_equal)
    {
        const T constant = residuals[0];
        const unsigned num_bytes = (bitWidth(constant) + 7u) / 8u; // 0..sizeof(T)
        out[0] = static_cast<uint8_t>(0x80u | num_bytes);
        storeLE(out + 1, static_cast<uint64_t>(constant), num_bytes);
        return 1u + num_bytes;
    }

    // Bit-width histogram; the count of values needing more than `width` bits is a suffix sum over it.
    unsigned histogram[typeBits<T> + 1] = {0};
    unsigned max_width = 0;
    for (unsigned i = 0; i < cnt; ++i)
    {
        const unsigned value_width = bitWidth(residuals[i]);
        ++histogram[value_width];
        max_width = std::max(value_width, max_width);
    }

    // A value is an exception exactly when its width exceeds the base, so the suffix sum is already
    // the winning width's exception count and is recorded here rather than recounted over the block.
    size_t best_cost = 2 + packedBytes(cnt, max_width);
    unsigned best_width = max_width;
    unsigned best_num_exceptions = 0;
    unsigned num_exceptions_at_width = 0;
    for (int width = static_cast<int>(max_width) - 1; width >= 0; --width)
    {
        num_exceptions_at_width += histogram[width + 1];
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

    const unsigned base_width = best_width;
    const unsigned num_exceptions = best_num_exceptions;

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
