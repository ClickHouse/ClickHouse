#include <Functions/FunctionBase32Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Base32Encode)
{
    factory.registerFunction<FunctionBaseXXConversion<BaseXXEncode<Base32Traits>>>();
}

}
