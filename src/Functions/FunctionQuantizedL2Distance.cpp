#include "FunctionQuantizedL2Distance.h"
#include "Functions/FunctionDequantize16Bit.h"
#include "Functions/FunctionDequantize8Bit.h"

namespace DB
{

REGISTER_FUNCTION(Quantized8BitL2Distance)
{
    static constexpr char name[] = "quantized8BitL2Distance";
    factory.registerFunction<FunctionQuantizedL2Distance<TargetSpecific::Default::Dequantize8BitImpl, name>>();
}

}
