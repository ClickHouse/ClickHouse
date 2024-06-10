#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
REGISTER_FUNCTION(TryBase64UrlDecode)
{
    factory.registerFunction<FunctionBase64Conversion<TryBase64Decode<Base64Variant::Url>>>();
}
}

#endif
