#include "FunctionQuantize1Bit.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <Functions/FunctionFactory.h>
#include <base/unaligned.h>

namespace DB
{

DECLARE_DEFAULT_CODE(

    template <typename FloatType> void Quantize1BitImpl::execute(const FloatType * input, UInt8 * output, size_t size) {
        size_t outIndex = 0;
        UInt8 currentByte = 0;
        int bitCount = 0;
        for (size_t i = 0; i < size; ++i)
        {
            UInt8 bit = (input[i] >= 0.0f) ? 1 : 0;
            currentByte |= (bit << bitCount);
            ++bitCount;

            if (bitCount == 8)
            {
                output[outIndex++] = currentByte;
                currentByte = 0;
                bitCount = 0;
            }
        }

        if (bitCount > 0)
        {
            output[outIndex++] = currentByte;
        }
    }

)

REGISTER_FUNCTION(Quantize1Bit)
{
    factory.registerFunction<FunctionQuantize1Bit>();
}

} // namespace DB
