#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBase64Conversion.h>

#include "config_functions.h"

#if USE_BASE64
#    include <DataTypes/DataTypeString.h>

namespace DB
{
REGISTER_FUNCTION(Base64Encode)
{
    tb64ini(0, 0);
    factory.registerFunction<FunctionBase64Conversion<Base64Encode>>();

    /// MysQL compatibility alias.
    factory.registerFunction<FunctionBase64Conversion<Base64Encode>>("TO_BASE64", FunctionFactory::CaseInsensitive);
}
}
#endif
