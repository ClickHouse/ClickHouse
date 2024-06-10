#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
REGISTER_FUNCTION(Base64UrlEncode)
{
    factory.registerFunction<FunctionBase64Conversion<Base64Encode<Base64Variant::Url>>>();
}
}

#endif
