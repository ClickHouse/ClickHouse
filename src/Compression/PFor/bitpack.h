#pragma once

// Little-endian, LSB-first bit packing of fixed-width integers. Independently authored.
// `b` is the per-value width in [0, typeBits<T>]; b == 0 packs/unpacks nothing.

#include <Compression/PFor/common.h>

namespace DB::PFor::detail
{

template <typename T>
inline ALWAYS_INLINE void packBits(const T * in, size_t n, unsigned b, uint8_t * out) noexcept
{
    if (b == 0)
        return;
    using W = Wide<T>;
    const T mask = lowMask<T>(b);
    W acc = 0;
    unsigned bits = 0;
    uint8_t * p = out;
    for (size_t i = 0; i < n; ++i)
    {
        acc |= static_cast<W>(in[i] & mask) << bits;
        bits += b;
        while (bits >= 8)
        {
            *p++ = static_cast<uint8_t>(acc);
            acc >>= 8;
            bits -= 8;
        }
    }
    if (bits)
        *p++ = static_cast<uint8_t>(acc);
}

// Unpack n values whose width B is a compile-time constant: the refill loop unrolls and
// the shift/mask fold to constants, several times faster than a runtime width.
template <typename T, unsigned B>
inline ALWAYS_INLINE void unpackFixed(const uint8_t * in, size_t n, T * out) noexcept
{
    using W = Wide<T>;
    const T mask = lowMask<T>(B);
    W acc = 0;
    unsigned bits = 0;
    const uint8_t * p = in;
    for (size_t i = 0; i < n; ++i)
    {
        while (bits < B)
        {
            acc |= static_cast<W>(*p++) << bits;
            bits += 8;
        }
        out[i] = static_cast<T>(acc & mask);
        acc >>= B;
        bits -= B;
    }
}

template <typename T>
inline ALWAYS_INLINE void unpackBits(const uint8_t * in, size_t n, unsigned b, T * out) noexcept
{
    switch (b)
    {
        case 0:
            for (size_t i = 0; i < n; ++i)
                out[i] = 0;
            return;
#define PFOR_UNPACK_CASE(K) \
        case (K): \
            if constexpr ((K) <= typeBits<T>) \
                unpackFixed<T, (((K) <= typeBits<T>) ? (K) : 1u)>(in, n, out); \
            return;
        PFOR_UNPACK_CASE(1)  PFOR_UNPACK_CASE(2)  PFOR_UNPACK_CASE(3)  PFOR_UNPACK_CASE(4)
        PFOR_UNPACK_CASE(5)  PFOR_UNPACK_CASE(6)  PFOR_UNPACK_CASE(7)  PFOR_UNPACK_CASE(8)
        PFOR_UNPACK_CASE(9)  PFOR_UNPACK_CASE(10) PFOR_UNPACK_CASE(11) PFOR_UNPACK_CASE(12)
        PFOR_UNPACK_CASE(13) PFOR_UNPACK_CASE(14) PFOR_UNPACK_CASE(15) PFOR_UNPACK_CASE(16)
        PFOR_UNPACK_CASE(17) PFOR_UNPACK_CASE(18) PFOR_UNPACK_CASE(19) PFOR_UNPACK_CASE(20)
        PFOR_UNPACK_CASE(21) PFOR_UNPACK_CASE(22) PFOR_UNPACK_CASE(23) PFOR_UNPACK_CASE(24)
        PFOR_UNPACK_CASE(25) PFOR_UNPACK_CASE(26) PFOR_UNPACK_CASE(27) PFOR_UNPACK_CASE(28)
        PFOR_UNPACK_CASE(29) PFOR_UNPACK_CASE(30) PFOR_UNPACK_CASE(31) PFOR_UNPACK_CASE(32)
        PFOR_UNPACK_CASE(33) PFOR_UNPACK_CASE(34) PFOR_UNPACK_CASE(35) PFOR_UNPACK_CASE(36)
        PFOR_UNPACK_CASE(37) PFOR_UNPACK_CASE(38) PFOR_UNPACK_CASE(39) PFOR_UNPACK_CASE(40)
        PFOR_UNPACK_CASE(41) PFOR_UNPACK_CASE(42) PFOR_UNPACK_CASE(43) PFOR_UNPACK_CASE(44)
        PFOR_UNPACK_CASE(45) PFOR_UNPACK_CASE(46) PFOR_UNPACK_CASE(47) PFOR_UNPACK_CASE(48)
        PFOR_UNPACK_CASE(49) PFOR_UNPACK_CASE(50) PFOR_UNPACK_CASE(51) PFOR_UNPACK_CASE(52)
        PFOR_UNPACK_CASE(53) PFOR_UNPACK_CASE(54) PFOR_UNPACK_CASE(55) PFOR_UNPACK_CASE(56)
        PFOR_UNPACK_CASE(57) PFOR_UNPACK_CASE(58) PFOR_UNPACK_CASE(59) PFOR_UNPACK_CASE(60)
        PFOR_UNPACK_CASE(61) PFOR_UNPACK_CASE(62) PFOR_UNPACK_CASE(63) PFOR_UNPACK_CASE(64)
#undef PFOR_UNPACK_CASE
        default:
            return;
    }
}

} // namespace DB::PFor::detail
