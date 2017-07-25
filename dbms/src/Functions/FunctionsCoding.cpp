#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsCoding.h>

namespace DB
{

void registerFunctionsCoding(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStringCutToZero>();
    factory.registerFunction<FunctionIPv6NumToString>();
    factory.registerFunction<FunctionCutIPv6>();
    factory.registerFunction<FunctionIPv6StringToNum>();
    factory.registerFunction<FunctionIPv4NumToString>();
    factory.registerFunction<FunctionIPv4StringToNum>();
    factory.registerFunction<FunctionIPv4NumToStringClassC>();
    factory.registerFunction<FunctionIPv4ToIPv6>();
    factory.registerFunction<FunctionMACNumToString>();
    factory.registerFunction<FunctionMACStringTo<ParseMACImpl>>();
    factory.registerFunction<FunctionMACStringTo<ParseOUIImpl>>();
    factory.registerFunction<FunctionUUIDNumToString>();
    factory.registerFunction<FunctionUUIDStringToNum>();
    factory.registerFunction<FunctionGenerateUUIDv4>();
    factory.registerFunction<FunctionHex>();
    factory.registerFunction<FunctionUnhex>();
    factory.registerFunction<FunctionBitmaskToArray>();
}

}
