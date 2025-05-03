#include <Functions/FunctionBase58Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Base58Encode)
{
    factory.registerFunction<FunctionBaseXXConversion<BaseXXEncode<Base58Traits>>>();
}

}
