#include "FunctionDequantize1Bit.h"
#include <cstring>
#include <immintrin.h>
#include "Common/TargetSpecific.h"

namespace DB
{

DECLARE_DEFAULT_CODE(

    void Dequantize1BitImpl::execute(const UInt8 * input, float * output, size_t size) {
        for (size_t i = 0; i < size; ++i)
        {
            size_t byteIndex = i / 8;
            size_t bitIndex = i % 8;
            UInt8 bit = (input[byteIndex] >> bitIndex) & 0x1;
            output[i] = (bit == 1) ? 1.0f : -1.0f;
        }
    }

)

REGISTER_FUNCTION(Dequantize1Bit)
{
    factory.registerFunction<FunctionDequantize1Bit>();
}

} // namespace DB
