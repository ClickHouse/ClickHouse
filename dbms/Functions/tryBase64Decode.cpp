#include <Functions/FunctionBase64Conversion.h>
#if USE_BASE64
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{
void registerFunctionTryBase64Decode(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBase64Conversion<TryBase64Decode>>();
}
}
#endif
