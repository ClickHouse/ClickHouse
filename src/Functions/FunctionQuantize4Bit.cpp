#include "FunctionQuantize4Bit.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <Functions/FunctionFactory.h>
#include <base/unaligned.h>
#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#    include <x86intrin.h>
#endif

namespace DB
{

DECLARE_DEFAULT_CODE(

    uint8_t Float32ToFloat4(float f) {
        if (std::isnan(f) || std::isinf(f))
        {
            f = 6.0f;
        }

        if (f > 6.0f)
        {
            f = 6.0f;
        }
        if (f < -6.0f)
        {
            f = -6.0f;
        }

        uint8_t sign = (f < 0) ? 1 : 0;
        float abs_f = std::fabs(f);

        if (abs_f == 0.0f)
        {
            return (sign << 3);
        }

        // The representable positive values for FLOAT4 (ignoring sign) are:
        //   0:  0          (code 000)
        //   1:  0.5        (code 001)
        //   2:  1.0        (code 010)
        //   3:  1.5        (code 011)
        //   4:  2.0        (code 100)
        //   5:  3.0        (code 101)
        //   6:  4.0        (code 110)
        //   7:  6.0        (code 111)
        //
        // Note: Zero is represented only by 000. For nonzero values we only consider
        // the seven codes 001, 010, 011, 100, 101, 110, 111.
        const float repValues[7] = {0.5f, 1.0f, 1.5f, 2.0f, 3.0f, 4.0f, 6.0f};
        // Corresponding codes (lower 3 bits) for nonzero values.
        const uint8_t codes[7] = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7};

        int bestIdx = 0;
        float bestDiff = std::fabs(abs_f - repValues[0]);
        for (int i = 1; i < 7; ++i)
        {
            float diff = std::fabs(abs_f - repValues[i]);
            if (diff < bestDiff)
            {
                bestDiff = diff;
                bestIdx = i;
            }
            else if (std::fabs(diff - bestDiff) < 1e-6f)
            {
                if ((codes[i] & 0x1) == 0)
                {
                    bestIdx = i;
                }
            }
        }
        uint8_t code = codes[bestIdx];
        return (sign << 3) | code;
    }

    void Quantize4BitImpl::execute(const Float32 * input, UInt8 * output, size_t size) {
        size_t outIndex = 0;
        size_t i = 0;

        for (; i + 1 < size; i += 2)
        {
            uint8_t nibble1 = Float32ToFloat4(input[i]) & 0x0F;
            uint8_t nibble2 = Float32ToFloat4(input[i + 1]) & 0x0F;
            output[outIndex++] = (nibble2 << 4) | nibble1;
        }

        if (i < size)
        {
            uint8_t nibble = Float32ToFloat4(input[i]) & 0x0F;
            output[outIndex++] = nibble;
        }
    }

    void Quantize4BitImpl::execute(const Float64 * input, UInt8 * output, size_t size) {
        size_t outIndex = 0;
        size_t i = 0;

        for (; i + 1 < size; i += 2)
        {
            uint8_t nibble1 = Float32ToFloat4(static_cast<float>(input[i])) & 0x0F;
            uint8_t nibble2 = Float32ToFloat4(static_cast<float>(input[i + 1])) & 0x0F;
            output[outIndex++] = (nibble2 << 4) | nibble1;
        }

        if (i < size)
        {
            uint8_t nibble = Float32ToFloat4(static_cast<float>(input[i])) & 0x0F;
            output[outIndex++] = nibble;
        }
    }

)

REGISTER_FUNCTION(Quantize4Bit)
{
    factory.registerFunction<FunctionQuantize4Bit>();
}

} // namespace DB
