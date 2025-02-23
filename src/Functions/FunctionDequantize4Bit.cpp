#include "FunctionDequantize4Bit.h"
#include <cstring>
#include <immintrin.h>
#include "Common/TargetSpecific.h"
#include "Functions/FunctionHelpers.h"

namespace DB
{

DECLARE_DEFAULT_CODE(

    void Dequantize4BitImpl::execute(const UInt8 * input, float * output, size_t size) {
        size_t outIndex = 0;
        size_t numPacked = size / 2;

        for (size_t i = 0; i < numPacked; ++i)
        {
            uint8_t packed = input[i];
            output[outIndex++] = Lookup4Bit::dequantize_lookup[packed & 0x0F];
            output[outIndex++] = Lookup4Bit::dequantize_lookup[packed >> 4];
        }

        if (size % 2 != 0)
        {
            uint8_t packed = input[numPacked];
            output[outIndex++] = Lookup4Bit::dequantize_lookup[packed & 0x0F];
        }
    }

)

REGISTER_FUNCTION(Dequantize4Bit)
{
    factory.registerFunction<FunctionDequantize4Bit>();
}

} // namespace DB
