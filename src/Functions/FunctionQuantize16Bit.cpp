#include "FunctionQuantize16Bit.h"
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <Functions/FunctionFactory.h>
#include <base/unaligned.h>
#include "Common/TargetSpecific.h"
#include "base/types.h"
#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#    include <x86intrin.h>
#endif

namespace DB
{

DECLARE_DEFAULT_CODE(

    static uint16_t Float32ToFloat16(float val) {
        uint32_t fbits = std::bit_cast<uint32_t>(val);
        uint32_t sign = (fbits >> 31) & 0x1;
        uint32_t exp = (fbits >> 23) & 0xFF;
        uint32_t frac = fbits & 0x7FFFFF;

        uint16_t hsign = static_cast<uint16_t>(sign);
        uint16_t hexp = 0;
        uint16_t hfrac = 0;

        if (exp == 0)
        {
            return (hsign << 15);
        }
        else if (exp == 0xFF)
        {
            hexp = 0x1F;
            hfrac = (frac != 0) ? 1 : 0;
            return (hsign << 15) | (hexp << 10) | hfrac;
        }

        int new_exp = static_cast<int>(exp) - 127;

        if (new_exp < -24)
        {
            return (hsign << 15);
        }
        else if (new_exp < -14)
        {
            uint32_t exp_val = static_cast<uint32_t>(-14 - new_exp);
            switch (exp_val)
            {
                case 0:
                    hfrac = 0;
                    break;
                case 1:
                    hfrac = 512 + (frac >> 14);
                    break;
                case 2:
                    hfrac = 256 + (frac >> 15);
                    break;
                case 3:
                    hfrac = 128 + (frac >> 16);
                    break;
                case 4:
                    hfrac = 64 + (frac >> 17);
                    break;
                case 5:
                    hfrac = 32 + (frac >> 18);
                    break;
                case 6:
                    hfrac = 16 + (frac >> 19);
                    break;
                case 7:
                    hfrac = 8 + (frac >> 20);
                    break;
                case 8:
                    hfrac = 4 + (frac >> 21);
                    break;
                case 9:
                    hfrac = 2 + (frac >> 22);
                    break;
                case 10:
                    hfrac = 1;
                    break;
                default:
                    hfrac = 0;
                    break;
            }
            return (hsign << 15) | (0 << 10) | hfrac;
        }
        else if (new_exp > 15)
        {
            hexp = 0x1F;
            hfrac = 0;
            return (hsign << 15) | (hexp << 10) | hfrac;
        }
        else
        {
            hexp = static_cast<uint16_t>(new_exp + 15);
            hfrac = static_cast<uint16_t>(frac >> 13);
            return (hsign << 15) | (hexp << 10) | hfrac;
        }
    }

    void Quantize16BitImpl::execute(const Float32 * input, UInt8 * output, size_t size) {
        auto out = reinterpret_cast<uint16_t *>(output);
        for (size_t i = 0; i < size; ++i)
        {
            out[i] = Float32ToFloat16(input[i]);
        }
    }

    void Quantize16BitImpl::execute(const Float64 * input, UInt8 * output, size_t size) {
        auto out = reinterpret_cast<uint16_t *>(output);
        for (size_t i = 0; i < size; ++i)
        {
            out[i] = Float32ToFloat16(static_cast<float>(input[i]));
        }
    })

REGISTER_FUNCTION(Quantize16Bit)
{
    factory.registerFunction<FunctionQuantize16Bit>();
}

} // namespace DB
