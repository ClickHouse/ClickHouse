#include "FunctionDequantize.h"
#include "Common/FunctionDocumentation.h"

namespace DB
{

struct Dequantize16BitImpl
{
    static void execute(const UInt8 * input, float * output, size_t size)
    {
        const uint16_t * in = reinterpret_cast<const uint16_t *>(input);
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = Lookup16Bit::dequantize_lookup[in[i]];
        }
    }
};

struct DequantizeSFP8BitImpl
{
    static void execute(const UInt8 * input, float * output, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = LookupSFP8Bit::dequantize_lookup[input[i]];
        }
    }
};

struct DequantizeMini8BitImpl
{
    static void execute(const UInt8 * input, float * output, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = LookupMini8Bit::dequantize_lookup[input[i]];
        }
    }
};

struct Dequantize4BitImpl
{
    static void execute(const UInt8 * input, float * output, size_t size)
    {
        size_t out_index = 0;
        size_t num_packed = size / 2;

        for (size_t i = 0; i < num_packed; ++i)
        {
            uint8_t packed = input[i];
            output[out_index++] = Lookup4Bit::dequantize_lookup[packed & 0x0F];
            output[out_index++] = Lookup4Bit::dequantize_lookup[packed >> 4];
        }

        if (size % 2 != 0)
        {
            uint8_t packed = input[num_packed];
            output[out_index++] = Lookup4Bit::dequantize_lookup[packed & 0x0F];
        }
    }
};

struct Dequantize1BitImpl
{
    static void execute(const UInt8 * input, float * output, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
        {
            size_t byte_index = i / 8;
            size_t bit_index = i % 8;
            UInt8 bit = (input[byte_index] >> bit_index) & 0x1;
            output[i] = (bit == 1) ? 1.0f : -1.0f;
        }
    }
};

struct Dequantize16BitTraits
{
    static constexpr const char * name = "dequantize16Bit";
    static constexpr size_t multiplier = 1;
    static constexpr size_t divisor = 2;
};

struct DequantizeSFP8BitTraits
{
    static constexpr const char * name = "dequantizeSFP8Bit";
    static constexpr size_t multiplier = 1;
    static constexpr size_t divisor = 1;
};

struct DequantizeMini8BitTraits
{
    static constexpr const char * name = "dequantizeMini8Bit";
    static constexpr size_t multiplier = 1;
    static constexpr size_t divisor = 1;
};

struct Dequantize4BitTraits
{
    static constexpr const char * name = "dequantize4Bit";
    static constexpr size_t multiplier = 2;
    static constexpr size_t divisor = 1;
};

struct Dequantize1BitTraits
{
    static constexpr const char * name = "dequantize1Bit";
    static constexpr size_t multiplier = 8;
    static constexpr size_t divisor = 1;
};

using FunctionDequantize16Bit = FunctionDequantizeBase<Dequantize16BitTraits, Dequantize16BitImpl>;
using FunctionDequantizeSFP8Bit = FunctionDequantizeBase<DequantizeSFP8BitTraits, DequantizeSFP8BitImpl>;
using FunctionDequantizeMini8Bit = FunctionDequantizeBase<DequantizeMini8BitTraits, DequantizeMini8BitImpl>;
using FunctionDequantize4Bit = FunctionDequantizeBase<Dequantize4BitTraits, Dequantize4BitImpl>;
using FunctionDequantize1Bit = FunctionDequantizeBase<Dequantize1BitTraits, Dequantize1BitImpl>;

REGISTER_FUNCTION(Dequantize16Bit)
{
    factory.registerFunction<FunctionDequantize16Bit>(FunctionDocumentation{.description = R"(Dequantize function.)"});
}

REGISTER_FUNCTION(DequantizeSFP8Bit)
{
    factory.registerFunction<FunctionDequantizeSFP8Bit>(FunctionDocumentation{.description = R"(Dequantize function.)"});
}

REGISTER_FUNCTION(DequantizeMini8Bit)
{
    factory.registerFunction<FunctionDequantizeMini8Bit>(FunctionDocumentation{.description = R"(Dequantize function.)"});
}

REGISTER_FUNCTION(Dequantize4Bit)
{
    factory.registerFunction<FunctionDequantize4Bit>(FunctionDocumentation{.description = R"(Dequantize function.)"});
}

REGISTER_FUNCTION(Dequantize1Bit)
{
    factory.registerFunction<FunctionDequantize1Bit>(FunctionDocumentation{.description = R"(Dequantize function.)"});
}

}
