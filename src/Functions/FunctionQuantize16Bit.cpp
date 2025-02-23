#include "FunctionQuantize16Bit.h"
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <Functions/FunctionFactory.h>
#include <base/unaligned.h>
#include "Common/TargetSpecific.h"
#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#    include <x86intrin.h>
#endif

namespace DB
{

DECLARE_DEFAULT_CODE(

    static uint16_t Float32ToFloat16(const float & val) {
        const uint32_t bits = std::bit_cast<uint32_t>(val);

        const uint16_t sign = (bits & 0x80000000) >> 16;
        const uint32_t frac32 = bits & 0x7fffff;
        const uint8_t exp32 = (bits & 0x7f800000) >> 23;
        const int8_t exp32_diff = exp32 - 127;

        uint16_t exp16 = 0;
        uint16_t frac16 = frac32 >> 13;

        if (__builtin_expect(exp32 == 0xff || exp32_diff > 15, 0))
        {
            exp16 = 0x1f;
        }
        else if (__builtin_expect(exp32 == 0 || exp32_diff < -14, 0))
        {
            exp16 = 0;
        }
        else
        {
            exp16 = exp32_diff + 15;
        }

        if (__builtin_expect(exp32 == 0xff && frac32 != 0 && frac16 == 0, 0))
        {
            frac16 = 0x200;
        }
        else if (__builtin_expect(exp32 == 0 || (exp16 == 0x1f && exp32 != 0xff), 0))
        {
            frac16 = 0;
        }
        else if (__builtin_expect(exp16 == 0 && exp32 != 0, 0))
        {
            frac16 = 0x100 | (frac16 >> 2);
        }

        uint16_t ret = 0;
        ret |= sign;
        ret |= exp16 << 10;
        ret |= frac16;

        return ret;
    }

    void Quantize16BitImpl::execute(const Float32 * input, UInt8 * output, size_t size) {
        auto out = reinterpret_cast<uint16_t *>(output);
        for (size_t i = 0; i < size; ++i)
        {
            out[i] = Float32ToFloat16(input[i]);
        }
    }

)

REGISTER_FUNCTION(Quantize16Bit)
{
    factory.registerFunction<FunctionQuantize16Bit>();
}

} // namespace DB
