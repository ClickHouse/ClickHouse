#pragma once

// One block of up to BLOCK residuals, encoded as frame-of-reference bit-packing with
// patched exceptions (the PForDelta scheme). Independently authored.
//
// Layout:
//   byte0 high bit set  -> constant block: low 7 bits = k value bytes; then k LE bytes.
//                          (k == 0 means the constant is 0.)
//   byte0 high bit clear -> normal block:
//        byte0 = b        base bit width (0..typeBits<T>)
//        byte1 = e        number of exceptions (values needing more than b bits)
//        byte2 = hb       (only if e > 0) high-bit width of the exception patches
//        base             cnt values, low b bits each, bit-packed
//        positions        e bytes (each < cnt), the exception indices
//        patches          e values, hb bits each, bit-packed; patch = value >> b
//
// Decode unpacks the base (low bits), then ORs each exception's high bits back in.
// b is chosen per block to minimise total bytes (base + exception overhead).
//
// The base of a full 128-value uint32 block with b in [1,31] uses the SIMD vertical
// layout (vertical.hpp); partial blocks, uint64, and b in {0,32} use the scalar packer.
// Both layouts occupy the same packedBytes(cnt,b) bytes, so the stream is identical in
// size and (T, cnt, b) selects the layout deterministically on both encode and decode.

#include <Compression/PFor/bitpack.h>
#include <Compression/PFor/common.h>
#include <Compression/PFor/vertical.h>

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
inline size_t blockEncode(const T * r, unsigned cnt, uint8_t * out) noexcept
{
    bool all_equal = true;
    for (unsigned i = 1; i < cnt; ++i)
        if (r[i] != r[0])
        {
            all_equal = false;
            break;
        }
    if (all_equal)
    {
        const T c = r[0];
        const unsigned k = (bitWidth(c) + 7u) / 8u; // 0..sizeof(T)
        out[0] = static_cast<uint8_t>(0x80u | k);
        storeLE(out + 1, static_cast<uint64_t>(c), k);
        return 1u + k;
    }

    // Bit-width histogram; e(b) = #values needing > b bits is a suffix sum over it.
    unsigned hist[typeBits<T> + 1] = {0};
    unsigned maxw = 0;
    for (unsigned i = 0; i < cnt; ++i)
    {
        const unsigned w = bitWidth(r[i]);
        ++hist[w];
        if (w > maxw)
            maxw = w;
    }

    // Candidate b == maxw has no exceptions; walk b down, adding exceptions, keep the min.
    size_t best_cost = 2 + packedBytes(cnt, maxw);
    unsigned best_b = maxw;
    unsigned e = 0;
    for (int b = static_cast<int>(maxw) - 1; b >= 0; --b)
    {
        e += hist[b + 1];
        const unsigned hb = maxw - static_cast<unsigned>(b);
        const size_t cost = 3 + packedBytes(cnt, static_cast<unsigned>(b)) + e + packedBytes(e, hb);
        if (cost < best_cost)
        {
            best_cost = cost;
            best_b = static_cast<unsigned>(b);
        }
    }

    const unsigned b = best_b;
    unsigned ecount = 0;
    // Exceptions exist only when b < maxw; then b < typeBits<T>, so r[i] >> b is defined
    // (a full-width block, b == maxw == typeBits<T>, has no exceptions).
    if (b < maxw)
        for (unsigned i = 0; i < cnt; ++i)
            if ((r[i] >> b) != 0)
                ++ecount;

    out[0] = static_cast<uint8_t>(b);
    out[1] = static_cast<uint8_t>(ecount);
    uint8_t * p = out + 2;
    unsigned hb = 0;
    if (ecount)
    {
        hb = maxw - b;
        *p++ = static_cast<uint8_t>(hb);
    }

    packBase<T>(r, cnt, b, p);
    p += packedBytes(cnt, b);

    if (ecount)
    {
        uint8_t * pos = p;
        p += ecount;
        T patches[BLOCK];
        unsigned j = 0;
        for (unsigned i = 0; i < cnt; ++i)
        {
            const T hi = static_cast<T>(r[i] >> b);
            if (hi)
            {
                pos[j] = static_cast<uint8_t>(i);
                patches[j] = hi;
                ++j;
            }
        }
        packBits<T>(patches, ecount, hb, p);
        p += packedBytes(ecount, hb);
    }
    return static_cast<size_t>(p - out);
}

// Reconstruct values from residuals already in `out` (inclusive prefix sum + running
// carry). SIMD for uint32 (deltaDecode32), scalar for uint64. plus is 0 for d0, 1 for d1.
template <typename T>
inline ALWAYS_INLINE void deltaApply(T * out, unsigned cnt, T & prev, uint32_t plus) noexcept
{
#if PFOR_HAS_VERTICAL
    if constexpr (sizeof(T) == 4)
    {
        uint32_t carry = static_cast<uint32_t>(prev);
        deltaDecode32(reinterpret_cast<uint32_t *>(out), cnt, carry, plus);
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

template <typename T>
inline size_t blockDecode(const uint8_t * in, unsigned cnt, T * out, Delta mode, T & prev) noexcept
{
    const uint32_t plus = (mode == Delta::d1) ? 1u : 0u;
    const uint8_t b0 = in[0];
    if (b0 & 0x80u)
    {
        const unsigned k = b0 & 0x7Fu;
        const T c = static_cast<T>(loadLE(in + 1, k));
        for (unsigned i = 0; i < cnt; ++i)
            out[i] = c;
        if (mode != Delta::none)
            deltaApply<T>(out, cnt, prev, plus);
        return 1u + k;
    }

    const unsigned b = b0;
    const unsigned e = in[1];
    const uint8_t * p = in + 2;
    unsigned hb = 0;
    if (e)
        hb = *p++;

    // Fused single pass: a full uint32 delta block with no exceptions unpacks and
    // prefix-sums in one sweep, with no second pass over the output.
#if PFOR_HAS_VERTICAL
    if constexpr (sizeof(T) == 4)
        if (mode != Delta::none && e == 0 && cnt == BLOCK && b >= 1 && b <= 31)
        {
            uint32_t carry = static_cast<uint32_t>(prev);
            unpackVertical32FusedDelta(p, b, reinterpret_cast<uint32_t *>(out), carry, plus);
            prev = static_cast<T>(carry);
            return static_cast<size_t>((p + packedBytes(cnt, b)) - in);
        }
#endif

    unpackBase<T>(p, cnt, b, out);
    p += packedBytes(cnt, b);

    if (e)
    {
        const uint8_t * pos = p;
        p += e;
        T patches[BLOCK];
        unpackBits<T>(p, e, hb, patches);
        p += packedBytes(e, hb);
        for (unsigned j = 0; j < e; ++j)
            out[pos[j]] |= static_cast<T>(patches[j]) << b;
    }
    if (mode != Delta::none)
        deltaApply<T>(out, cnt, prev, plus);
    return static_cast<size_t>(p - in);
}

}
