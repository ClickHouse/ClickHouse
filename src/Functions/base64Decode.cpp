#include <Functions/FunctionBase64Conversion.h>
#if USE_BASE64
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{
void registerFunctionBase64Decode(FunctionFactory & factory)
{
    tb64ini(0, 0);
    factory.registerFunction<FunctionBase64Conversion<Base64Decode>>();

    /// MysQL compatibility alias.
    factory.registerFunction<FunctionBase64Conversion<Base64Decode>>("FROM_BASE64", FunctionFactory::CaseInsensitive);
}
}
#endif
