#include <Functions/FunctionBase58Conversion.h>
#if USE_BASE58
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{
void registerFunctionBase58Encode(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBase58Conversion<Base58Encode>>();
}

void registerFunctionBase58Decode(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBase58Conversion<Base58Decode>>();
}

void registerFunctionTryBase58Decode(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBase58Conversion<TryBase58Decode>>();
}
}
#endif
