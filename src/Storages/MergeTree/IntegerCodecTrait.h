#pragma once
#if defined(__x86_64__) || defined(_M_X64)
#if defined(__SSE4_1__)
#define USE_SIMDCOMP 1
#endif
#endif

extern "C"
{
#if defined(USE_SIMDCOMP)
#include <simdcomp.h>
#endif
}

namespace DB
{

/// Block codec used by PostingsContainerImpl to compress/decompress arrays of
/// unsigned integers (typically delta/gap values).
struct BlockCodec
{
    static constexpr size_t kBlockSize = 128;

    /// Returns {compressed_bytes, bits} where bits is the max bit-width required
    /// to represent all values in [0..n).
    ALWAYS_INLINE static std::pair<size_t, size_t> evaluateSizeAndMaxBits([[maybe_unused]] const uint32_t * data, size_t n)
    {
#if defined(USE_SIMDCOMP)
        auto bits = maxbits_length(data, n);
        auto bytes = simdpack_compressedbytes(n, bits);
        return {bytes, bits};
#else
        auto bits = maxBits(data, n);
        auto bytes = static_cast<uint32_t>((n * static_cast<size_t>(bits) + 7) / 8);
        return {bytes, bits};
#endif
    }

    ALWAYS_INLINE static uint32_t encode(uint32_t *in, std::size_t n, [[maybe_unused]] uint32_t bits, unsigned char * & out)
    {
#if defined(USE_SIMDCOMP)
        /// simdcomp expects __m128i* output pointer; we compute consumed bytes
        /// from the returned end pointer (in units of 16-byte vectors).
        auto *m128_out = reinterpret_cast<__m128i *>(out);
        auto *m128_out_end = simdpack_length(in, n, m128_out, bits);
        auto used = static_cast<size_t>(m128_out_end - m128_out) * sizeof(__m128i);
        out += used;
        return used;
#else
        if (n != kBlockSize)
            return encodeVariadic(in, n, bits, out);
        return encodeGeneric(in, bits, out);
#endif
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char * & in, std::size_t n, [[maybe_unused]] uint32_t bits, uint32_t * out)
    {
#if defined(USE_SIMDCOMP)
        /// simdcomp expects __m128i* input pointer; we compute consumed bytes
        /// from the returned end pointer (in units of 16-byte vectors).
        auto *m128i_p = reinterpret_cast<__m128i *>(in);
        auto *m128i_p_end = simdunpack_length(m128i_p, n, out, bits);
        auto used = static_cast<size_t>(m128i_p_end - m128i_p) * sizeof(__m128);
        in += used;
        return used;
#else
        /// Fallback: optimized path for full 128-value blocks, generic otherwise.
        if (n != kBlockSize)
            return decodeVariadic(in, n, bits, out);
        return decodeGeneric(in, bits, out);
#endif
    }

    /// If the target platform is __x86_64__ or _M_X64, and only the SSE4.1 instruction set is available,
    /// we use simdcomp to encode/decode unsigned integer arrays.
    /// In all other cases, we fall back to the regular (non-SIMD) implementation.
#if !defined(USE_SIMDCOMP)
    static uint32_t maxBits(const uint32_t * data, size_t size)
    {
        uint32_t acc = 0;
        for (size_t i = 0; i < size; ++i)
            acc |= data[i];
        if (acc == 0) return 0;
        return 32u - static_cast<uint32_t>(__builtin_clz(acc));
    }

    static uint32_t encodeGeneric(const uint32_t * in, uint32_t bits, unsigned char * & out)
    {
        chassert(bits <= 32);

        if (bits == 0)
            return 0;

        const uint32_t m = (bits == 32) ? 0xFFFF'FFFFu : ((1u << bits) - 1u);

        if (bits == 32)
        {
            __builtin_memcpy(out, in, 128u * sizeof(uint32_t));
            out += 128u * sizeof(uint32_t);
            return 128u * sizeof(uint32_t);
        }

        uint64_t acc = 0;
        uint32_t acc_bits = 0;
        uint32_t out_pos = 0;

        for (uint32_t i = 0; i < 128; ++i)
        {
            uint32_t v = in[i] & m;

            while (acc_bits + bits > 64)
            {
                out[out_pos++] = static_cast<unsigned char>(acc & 0xFFu);
                acc >>= 8;
                acc_bits -= 8;
            }

            acc |= (static_cast<uint64_t>(v) << acc_bits);
            acc_bits += bits;

            while (acc_bits >= 8)
            {
                out[out_pos++] = static_cast<unsigned char>(acc & 0xFFu);
                acc >>= 8;
                acc_bits -= 8;
            }
        }

        if (acc_bits)
            out[out_pos++] = static_cast<unsigned char>(acc & 0xFFu);

        out += out_pos;
        return out_pos;
    }

    static uint32_t decodeGeneric(unsigned char * & in, uint32_t bits, uint32_t * out)
    {
        chassert(bits <= 32);

        const uint32_t need = static_cast<uint32_t>((128u * static_cast<uint64_t>(bits) + 7u) / 8u);

        if (bits == 0)
            return 0;

        const uint32_t m = (bits == 32) ? 0xFFFF'FFFFu : ((1u << bits) - 1u);

        if (bits == 32)
        {
            __builtin_memcpy(out, in, 128u * sizeof(uint32_t));
            in += 128u * sizeof(uint32_t);
            return 128u * sizeof(uint32_t);
        }

        uint64_t acc = 0;
        uint32_t acc_bits = 0;
        uint32_t in_pos = 0;

        for (uint32_t i = 0; i < 128; ++i)
        {
            while (acc_bits < bits)
            {
                acc |= (static_cast<uint64_t>(in[in_pos++]) << acc_bits);
                acc_bits += 8;
            }

            out[i] = static_cast<uint32_t>(acc) & m;
            acc >>= bits;
            acc_bits -= bits;
        }

        in += need;
        return need;
    }

    static uint32_t encodeVariadic(const uint32_t * in, size_t n, uint32_t bits, unsigned char * & out)
    {
        chassert(n < 128);
        if (bits == 0) return 0;

        const uint32_t mask = (1u << bits) - 1u;
        uint64_t acc = 0;
        uint32_t acc_bits = 0;
        uint32_t out_pos = 0;

        for (size_t i = 0; i < n; ++i)
        {
            uint32_t v = in[i] & mask;
            acc |= (static_cast<uint64_t>(v) << acc_bits);
            acc_bits += bits;

            while (acc_bits >= 8)
            {
                out[out_pos++] = static_cast<uint8_t>(acc & 0xFFu);
                acc >>= 8;
                acc_bits -= 8;
            }
        }

        if (acc_bits > 0)
            out[out_pos++] = static_cast<uint8_t>(acc & 0xFFu);
        out += out_pos;
        return out_pos;
    }

    static uint32_t decodeVariadic(unsigned char * & in, size_t n, uint32_t bits, uint32_t * out)
    {
        if (bits == 0)
            return 0;
        const uint32_t need = static_cast<uint32_t>((n * static_cast<size_t>(bits) + 7) / 8);
        const uint32_t mask = (1u << bits) - 1u;
        uint64_t acc = 0;
        uint32_t acc_bits = 0;
        uint32_t in_pos = 0;

        for (size_t i = 0; i < n; ++i)
        {
            while (acc_bits < bits)
            {
                acc |= (static_cast<uint64_t>(in[in_pos++]) << acc_bits);
                acc_bits += 8;
            }
            out[i] = static_cast<uint32_t>(acc) & mask;
            acc >>= bits;
            acc_bits -= bits;
        }
        in += need;
        return need;
    }
#endif
};

}
