#include <Functions/FunctionBase58Conversion.h>
#include <Functions/FunctionFactory.h>

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
}
