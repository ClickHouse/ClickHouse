#include "FunctionQuantizedL2Distance.h"
#include "Functions/FunctionDequantize16Bit.h"
#include "Functions/FunctionDequantize8Bit.h"
#include "base/types.h"

#ifdef ENABLE_MULTITARGET_CODE
#    include <immintrin.h>
#endif

namespace DB
{

REGISTER_FUNCTION(Quantized16BitL2Distance)
{
    static constexpr char name[] = "quantized16BitL2Distance";
    factory.registerFunction<FunctionQuantizedL2Distance<L2Distance16Bit, UInt16, name>>();
}

REGISTER_FUNCTION(Quantized8BitL2Distance)
{
    static constexpr char name[] = "quantized8BitL2Distance";
    factory.registerFunction<FunctionQuantizedL2Distance<L2Distance8Bit, UInt8, name>>();
}

}
