#include "FunctionQuantizedL2Distance.h"

namespace DB
{

FunctionPtr FunctionQuantizedL2Distance::dequantize_function;
FunctionPtr FunctionQuantizedL2Distance::l2_distance_function;

REGISTER_FUNCTION(QuantizedL2Distance)
{
    factory.registerFunction<FunctionQuantizedL2Distance>();
}

}
