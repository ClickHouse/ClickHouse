#pragma once

#include <cstdint>
#include <bit>


inline float convertFloat16ToFloat32(uint16_t float16_value)
{
    uint32_t old_sign = (float16_value & 0b10000000'00000000);
    uint32_t old_exponent = (float16_value & 0b01111100'00000000) >> 10;
    uint32_t old_mantissa = float16_value & 0b00000011'11111111;

    uint32_t new_exponent;
    uint32_t new_mantissa;
    uint32_t new_sign = old_sign << 16;

    if (unlikely(old_exponent == 0x1F))
    {
        /// Inf, NaN
        new_exponent = 0xFFu << 23;
        new_mantissa = old_mantissa << 13;
    }
    else if (old_exponent == 0)
    {
        if (likely(old_mantissa == 0))
        {
            /// Zeros
            new_exponent = 0;
            new_mantissa = 0;
        }
        else
        {
            /// Subnormals
            uint32_t adjustment = __builtin_clz(old_mantissa) - 22;
            new_exponent = (112 - adjustment) << 23;
            new_mantissa = (old_mantissa ^ (1 << (9 - adjustment))) << 13 << adjustment;
        }
    }
    else
    {
        /// Normals
        new_exponent = (old_exponent + 112) << 23;
        new_mantissa = old_mantissa << 13;
    }

    uint32_t float32_value = new_sign | new_exponent | new_mantissa;
    return std::bit_cast<float>(float32_value);
}
