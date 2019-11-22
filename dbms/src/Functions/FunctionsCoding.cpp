#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsCoding.h>

namespace DB
{

struct NameFunctionIPv4NumToString { static constexpr auto name = "IPv4NumToString"; };
struct NameFunctionIPv4NumToStringClassC { static constexpr auto name = "IPv4NumToStringClassC"; };


void registerFunctionsCoding(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStringCutToZero>();
    factory.registerFunction<FunctionIPv6NumToString>();
    factory.registerFunction<FunctionCutIPv6>();
    factory.registerFunction<FunctionIPv6StringToNum>();
    factory.registerFunction<FunctionIPv4NumToString<0, NameFunctionIPv4NumToString>>();
    factory.registerFunction<FunctionIPv4NumToString<1, NameFunctionIPv4NumToStringClassC>>();
    factory.registerFunction<FunctionIPv4StringToNum>();
    factory.registerFunction<FunctionIPv4ToIPv6>();
    factory.registerFunction<FunctionMACNumToString>();
    factory.registerFunction<FunctionMACStringTo<ParseMACImpl>>();
    factory.registerFunction<FunctionMACStringTo<ParseOUIImpl>>();
    factory.registerFunction<FunctionUUIDNumToString>();
    factory.registerFunction<FunctionUUIDStringToNum>();
    factory.registerFunction<FunctionHex>();
    factory.registerFunction<FunctionUnhex>();
    factory.registerFunction<FunctionBitmaskToArray>();
    factory.registerFunction<FunctionToIPv4>();
    factory.registerFunction<FunctionToIPv6>();
    factory.registerFunction<FunctionIPv6CIDRToRange>();
    factory.registerFunction<FunctionIPv4CIDRToRange>();
}

}
