#include "FunctionQuantize.h"

namespace DB
{

struct Quantize16BitImpl
{
    static uint16_t float32ToFloat16(float val)
    {
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

    static void execute(const Float32 * input, UInt8 * output, size_t size)
    {
        uint16_t * out = reinterpret_cast<uint16_t *>(output);
        for (size_t i = 0; i < size; ++i)
        {
            out[i] = float32ToFloat16(input[i]);
        }
    }

    static void execute(const Float64 * input, UInt8 * output, size_t size)
    {
        uint16_t * out = reinterpret_cast<uint16_t *>(output);
        for (size_t i = 0; i < size; ++i)
        {
            out[i] = float32ToFloat16(static_cast<float>(input[i]));
        }
    }
};

struct Quantize8BitImpl
{
    static uint32_t float32ToFloat8(float f)
    {
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

    static void execute(const Float32 * input, UInt8 * output, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = static_cast<uint8_t>(float32ToFloat8(input[i]));
        }
    }

    static void execute(const Float64 * input, UInt8 * output, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = static_cast<uint8_t>(float32ToFloat8(static_cast<float>(input[i])));
        }
    }
};

struct Quantize4BitImpl
{
    static uint8_t float32ToFloat4(float f)
    {
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
        const float rep_values[7] = {0.5f, 1.0f, 1.5f, 2.0f, 3.0f, 4.0f, 6.0f};
        // Corresponding codes (lower 3 bits) for nonzero values.
        const uint8_t codes[7] = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7};

        int best_idx = 0;
        float best_diff = std::fabs(abs_f - rep_values[0]);
        for (int i = 1; i < 7; ++i)
        {
            float diff = std::fabs(abs_f - rep_values[i]);
            if (diff < best_diff)
            {
                best_diff = diff;
                best_idx = i;
            }
            else if (std::fabs(diff - best_diff) < 1e-6f)
            {
                if ((codes[i] & 0x1) == 0)
                {
                    best_idx = i;
                }
            }
        }
        uint8_t code = codes[best_idx];
        return (sign << 3) | code;
    }

    static void execute(const Float32 * input, UInt8 * output, size_t size)
    {
        size_t out_index = 0;
        size_t i = 0;

        for (; i + 1 < size; i += 2)
        {
            uint8_t nibble1 = float32ToFloat4(input[i]) & 0x0F;
            uint8_t nibble2 = float32ToFloat4(input[i + 1]) & 0x0F;
            output[out_index++] = (nibble2 << 4) | nibble1;
        }

        if (i < size)
        {
            uint8_t nibble = float32ToFloat4(input[i]) & 0x0F;
            output[out_index++] = nibble;
        }
    }

    static void execute(const Float64 * input, UInt8 * output, size_t size)
    {
        size_t out_index = 0;
        size_t i = 0;

        for (; i + 1 < size; i += 2)
        {
            uint8_t nibble1 = float32ToFloat4(static_cast<float>(input[i])) & 0x0F;
            uint8_t nibble2 = float32ToFloat4(static_cast<float>(input[i + 1])) & 0x0F;
            output[out_index++] = (nibble2 << 4) | nibble1;
        }

        if (i < size)
        {
            uint8_t nibble = float32ToFloat4(static_cast<float>(input[i])) & 0x0F;
            output[out_index++] = nibble;
        }
    }
};

struct Quantize1BitImpl
{
    template <typename FloatType>
    static void execute(const FloatType * input, UInt8 * output, size_t size)
    {
        size_t out_index = 0;
        UInt8 current_byte = 0;
        int bit_count = 0;
        for (size_t i = 0; i < size; ++i)
        {
            UInt8 bit = (input[i] >= 0.0f) ? 1 : 0;
            current_byte |= (bit << bit_count);
            ++bit_count;

            if (bit_count == 8)
            {
                output[out_index++] = current_byte;
                current_byte = 0;
                bit_count = 0;
            }
        }

        if (bit_count > 0)
        {
            output[out_index++] = current_byte;
        }
    }
};

struct Quantize16BitTraits
{
    static constexpr const char * name = "quantize16Bit";
    static constexpr size_t multiplier = 2;
    static constexpr size_t divider = 1;
};

struct Quantize8BitTraits
{
    static constexpr const char * name = "quantize8Bit";
    static constexpr size_t multiplier = 1;
    static constexpr size_t divider = 1;
};

struct Quantize4BitTraits
{
    static constexpr const char * name = "quantize4Bit";
    static constexpr size_t multiplier = 1;
    static constexpr size_t divider = 2;
};

struct Quantize1BitTraits
{
    static constexpr const char * name = "quantize1Bit";
    static constexpr size_t multiplier = 1;
    static constexpr size_t divider = 8;
};

using FunctionQuantize16Bit = FunctionQuantizeBase<Quantize16BitTraits, Quantize16BitImpl>;
using FunctionQuantize8Bit = FunctionQuantizeBase<Quantize8BitTraits, Quantize8BitImpl>;
using FunctionQuantize4Bit = FunctionQuantizeBase<Quantize4BitTraits, Quantize4BitImpl>;
using FunctionQuantize1Bit = FunctionQuantizeBase<Quantize1BitTraits, Quantize1BitImpl>;

REGISTER_FUNCTION(Quantize16Bit)
{
    factory.registerFunction<FunctionQuantize16Bit>();
}

REGISTER_FUNCTION(Quantize8Bit)
{
    factory.registerFunction<FunctionQuantize8Bit>();
}

REGISTER_FUNCTION(Quantize4Bit)
{
    factory.registerFunction<FunctionQuantize4Bit>();
}

REGISTER_FUNCTION(Quantize1Bit)
{
    factory.registerFunction<FunctionQuantize1Bit>();
}

}
