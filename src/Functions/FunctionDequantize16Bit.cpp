#include "FunctionDequantize16Bit.h"
#include <cstring>
#include <immintrin.h>
#include "Common/TargetSpecific.h"
#include "Functions/FunctionHelpers.h"

namespace DB
{

DECLARE_DEFAULT_CODE(

    void Dequantize16BitImpl::execute(const UInt8 * input, float * output, size_t size) {
        auto in = reinterpret_cast<const uint16_t *>(input);
        for (size_t i = 0; i < size; ++i)
        {
            output[i] = Lookup16Bit::dequantize_lookup[in[i]];
        }
    }

)

REGISTER_FUNCTION(Dequantize16Bit)
{
    factory.registerFunction<FunctionDequantize16Bit>();
}

} // namespace DB
