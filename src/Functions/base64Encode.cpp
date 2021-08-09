#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBase64Conversion.h>

#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_BASE64
#    include <DataTypes/DataTypeString.h>

namespace DB
{
void registerFunctionBase64Encode(FunctionFactory & factory)
{
    tb64ini(0, 1);
    factory.registerFunction<FunctionBase64Conversion<Base64Encode>>();

    /// MysQL compatibility alias.
    factory.registerFunction<FunctionBase64Conversion<Base64Encode>>("TO_BASE64", FunctionFactory::CaseInsensitive);
}
}
#endif
