#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
REGISTER_FUNCTION(Base64Decode)
{
    factory.registerFunction<FunctionBase64Conversion<Base64Decode>>();

    /// MySQL compatibility alias.
    factory.registerAlias("FROM_BASE64", "base64Decode", FunctionFactory::CaseInsensitive);
}
}

#endif
