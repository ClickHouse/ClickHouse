#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBase64Conversion.h>

namespace DB
{

void registerFunctionBase64Decode(FunctionFactory &factory)
{
    factory.registerFunction<FunctionBase64Conversion<Base64Decode>>();
}
}