#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
REGISTER_FUNCTION(Base64Encode)
{
    tb64ini(0, 0);
    factory.registerFunction<FunctionBase64Conversion<Base64Encode>>();

    /// MysQL compatibility alias.
    factory.registerAlias("TO_BASE64", "base64Encode", FunctionFactory::CaseInsensitive);
}
}

#endif
