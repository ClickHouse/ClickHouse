#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
REGISTER_FUNCTION(TryBase64Decode)
{
    factory.registerFunction<FunctionBase64Conversion<TryBase64Decode>>();
}
}

#endif
