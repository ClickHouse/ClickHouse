#include "FunctionQuantize8Bit.h"
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

    uint32_t Float32ToFloat8(float f) {
        constexpr uint32_t k_mask_m = 0x007FFFFFu;
        uint32_t binary32;
        __builtin_memcpy(&binary32, &f, sizeof(float));
        const uint32_t s = (binary32 >> 24) & 0x80;
        binary32 &= 0x7FFFFFFF;
        __builtin_memcpy(&f, &binary32, sizeof(uint32_t));

        bool large_e = (f >= 0.007568359375f);
        bool round_1_111 = (f >= 0.00732421875f);

        const uint32_t m32 = binary32 & k_mask_m;
        const size_t m_bits = large_e + 2;
        const uint32_t is_odd = (m32 >> (23 - m_bits)) & 1;
        const uint32_t round = is_odd + (1u << (22 - m_bits)) - 1;
        const uint32_t rounded = binary32 + round;

        const size_t final_m_bits = (round_1_111 * 3) | (!round_1_111 * m_bits);

        uint32_t m = (k_mask_m & rounded) >> (23 - final_m_bits);
        int32_t e_p23 = (rounded >> 23) - 104;

        bool cond = !(e_p23 | m);
        m = cond | (!cond * m);

        uint32_t e_sfp = e_p23 - round_1_111 * 8;

        const uint32_t encoded = -(e_p23 >= 0) & ((e_sfp << final_m_bits) | m | s);
        return encoded;
    }

    void Quantize8BitImpl::execute(const Float32 * input, UInt8 * output, size_t size) {
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = static_cast<uint8_t>(Float32ToFloat8(input[i]));
        }
    }

    void Quantize8BitImpl::execute(const Float64 * input, UInt8 * output, size_t size) {
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = static_cast<uint8_t>(Float32ToFloat8(static_cast<float>(input[i])));
        }
    }

)

REGISTER_FUNCTION(Quantize8Bit)
{
    factory.registerFunction<FunctionQuantize8Bit>();
}

} // namespace DB
