#include "FunctionDequantize8Bit.h"
#include <cstring>
#include <immintrin.h>
#include "Common/TargetSpecific.h"
#include "Functions/FunctionHelpers.h"

namespace DB
{

DECLARE_DEFAULT_CODE(

    void Dequantize8BitImpl::execute(const UInt8 * input, float * output, size_t size) {
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = Lookup8Bit::dequantize_lookup[input[i]];
        }
    }

)

REGISTER_FUNCTION(Dequantize8Bit)
{
    factory.registerFunction<FunctionDequantize8Bit>();
}

} // namespace DB
