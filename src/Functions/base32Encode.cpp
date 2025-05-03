#include <Functions/FunctionBase32Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Base32Encode)
{
    factory.registerFunction<FunctionBase32Conversion<Base32Encode>>();
}

}
