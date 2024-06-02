#pragma once

#include <base/bit_cast.h>


using BFloat16 = __bf16;

namespace std
{
    inline constexpr bool isfinite(BFloat16 x) { return (bit_cast<UInt16>(x) & 0b0111111110000000) != 0b0111111110000000; }
    inline constexpr bool signbit(BFloat16 x) { return bit_cast<UInt16>(x) & 0b1000000000000000; }
}

inline Float32 BFloat16ToFloat32(BFloat16 x)
{
    return bit_cast<Float32>(static_cast<UInt32>(bit_cast<UInt16>(x)) << 16);
}

inline BFloat16 Float32ToBFloat16(Float32 x)
{
    return bit_cast<BFloat16>(std::bit_cast<UInt32>(x) >> 16);
}
