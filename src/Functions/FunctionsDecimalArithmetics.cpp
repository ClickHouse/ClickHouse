#include <Functions/FunctionsDecimalArithmetics.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
REGISTER_FUNCTION(DivideDecimals)
{
    factory.registerFunction<FunctionsDecimalArithmetics<DivideDecimalsImpl>>();
}

REGISTER_FUNCTION(MultiplyDecimals)
{
    factory.registerFunction<FunctionsDecimalArithmetics<MultiplyDecimalsImpl>>();
}
}
