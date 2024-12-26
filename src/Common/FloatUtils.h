#pragma once

#include <cstring>
#include <cmath>
#include <cstdint>
#include <algorithm>
#include <bit>


inline float convertFloat16ToFloat32(uint16_t float16_value)
{
    uint16_t sign = (float16_value >> 15) & 0x1;
    uint16_t exponent = (float16_value >> 10) & 0x1F;
    uint16_t fraction = float16_value & 0x3FF;

    if (exponent == 0 && fraction == 0)
    {
        uint32_t float32_value = sign << 31;
        return std::bit_cast<float>(float32_value);
    }

    // Handling special cases for exponent
    if (exponent == 0x1F)
    {
        // NaN or Infinity in float16
        return (fraction == 0) ? std::numeric_limits<float>::infinity() : std::numeric_limits<float>::quiet_NaN();
    }

    // Convert exponent from float16 to float32 format
    int32_t new_exponent = static_cast<int32_t>(exponent) - 15 + 127;

    // Constructing the float32 representation
    uint32_t float32_value = (static_cast<uint32_t>(sign) << 31) |
                             (static_cast<uint32_t>(new_exponent) << 23) |
                             (static_cast<uint32_t>(fraction) << 13);

    // Interpret the binary representation as a float
    float result;
    memcpy(&result, &float32_value, sizeof(float));

    // Determine decimal places dynamically based on the magnitude of the number
    int decimal_places = std::max(0, 6 - static_cast<int>(std::log10(std::abs(result))));
    // Truncate the decimal part to the determined number of decimal places
    float multiplier = static_cast<float>(std::pow(10.0f, decimal_places));
    result = std::round(result * multiplier) / multiplier;

    return result;
}
