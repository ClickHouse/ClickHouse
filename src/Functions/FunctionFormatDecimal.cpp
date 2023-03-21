#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionFormatDecimal.h>

namespace DB
{

REGISTER_FUNCTION(FormatDecimal)
{
    factory.registerFunction<FunctionFormatDecimal>();
}

}


