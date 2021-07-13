#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsCoding.h>


namespace DB
{

struct NameFunctionIPv4NumToString { static constexpr auto name = "IPv4NumToString"; };
struct NameFunctionIPv4NumToStringClassC { static constexpr auto name = "IPv4NumToStringClassC"; };


void registerFunctionsCoding(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStringCutToZero>();
    factory.registerFunction<FunctionCutIPv6>();
    factory.registerFunction<FunctionIPv4ToIPv6>();
    factory.registerFunction<FunctionMACNumToString>();
    factory.registerFunction<FunctionMACStringTo<ParseMACImpl>>();
    factory.registerFunction<FunctionMACStringTo<ParseOUIImpl>>();
    factory.registerFunction<FunctionUUIDNumToString>();
    factory.registerFunction<FunctionUUIDStringToNum>();
    factory.registerFunction<FunctionHex>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionUnhex>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionBin>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionUnbin>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionChar>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionBitmaskToArray>();
    factory.registerFunction<FunctionBitPositionsToArray>();
    factory.registerFunction<FunctionToIPv4>();
    factory.registerFunction<FunctionToIPv6>();
    factory.registerFunction<FunctionIPv6CIDRToRange>();
    factory.registerFunction<FunctionIPv4CIDRToRange>();
    factory.registerFunction<FunctionIsIPv4String>();
    factory.registerFunction<FunctionIsIPv6String>();

    factory.registerFunction<FunctionIPv4NumToString<0, NameFunctionIPv4NumToString>>();
    factory.registerFunction<FunctionIPv4NumToString<1, NameFunctionIPv4NumToStringClassC>>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionIPv4NumToString<0, NameFunctionIPv4NumToString>>("INET_NTOA", FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionIPv4StringToNum>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionIPv4StringToNum>("INET_ATON", FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionIPv6NumToString>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionIPv6NumToString>("INET6_NTOA", FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionIPv6StringToNum>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionIPv6StringToNum>("INET6_ATON", FunctionFactory::CaseInsensitive);
}

}
