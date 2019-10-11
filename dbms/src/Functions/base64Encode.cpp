#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBase64Conversion.h>
#if USE_BASE64
#include <DataTypes/DataTypeString.h>

namespace DB
{
void registerFunctionBase64Encode(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBase64Conversion<Base64Encode>>();
}
}
#endif
