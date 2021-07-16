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
    factory.registerFunction<FunctionToIPv4>();
    factory.registerFunction<FunctionToIPv6>();
    factory.registerFunction<FunctionIPv6CIDRToRange>();
    factory.registerFunction<FunctionIPv4CIDRToRange>();
    factory.registerFunction<FunctionIsIPv4String>();
    factory.registerFunction<FunctionIsIPv6String>();

    factory.registerFunction<FunctionIPv4NumToString<0, NameFunctionIPv4NumToString>>();
    factory.registerFunction<FunctionIPv4NumToString<1, NameFunctionIPv4NumToStringClassC>>();

    factory.registerFunction<FunctionIPv4StringToNum>();
    factory.registerFunction<FunctionIPv6NumToString>();
    factory.registerFunction<FunctionIPv6StringToNum>();

    /// MysQL compatibility aliases:
    factory.registerAlias("INET_ATON", FunctionIPv4StringToNum::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("INET6_NTOA", FunctionIPv6NumToString::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("INET6_ATON", FunctionIPv6StringToNum::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("INET_NTOA", NameFunctionIPv4NumToString::name, FunctionFactory::CaseInsensitive);
}

}
