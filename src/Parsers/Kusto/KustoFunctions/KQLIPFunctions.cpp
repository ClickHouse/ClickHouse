#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/ParserSetQuery.h>

#include <format>

namespace DB::ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace DB
{

bool Ipv4Compare::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool Ipv4IsInRange::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    ++pos;

    const auto ip_address = getConvertedArgument(function_name, pos);
    ++pos;

    const auto ip_range = getConvertedArgument(function_name, pos);
    out = std::format("isIPAddressInRange({0}, concat({1}, if(position({1}, '/') > 0, '', '/32')))", ip_address, ip_range);
    return true;
}

bool Ipv4IsMatch::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool Ipv4IsPrivate::convertImpl(String & out, IParser::Pos & pos)
{
    static const std::array<String, 3> PRIVATE_SUBNETS{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"};

    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    ++pos;

    const auto ip_address = getConvertedArgument(function_name, pos);

    out += std::format(
        "multiIf(length(splitByChar('/', {0}) as tokens) > 2 or isNull(toIPv4OrNull(tokens[1]) as nullable_ip), null, "
        "length(tokens) = 2 and isNull(toUInt8OrNull(tokens[-1]) as suffix), null, "
        "ignore(assumeNotNull(nullable_ip) as ip, "
        "IPv4CIDRToRange(ip, assumeNotNull(suffix)) as range, IPv4NumToString(tupleElement(range, 1)) as begin, "
        "IPv4NumToString(tupleElement(range, 2)) as end), null, ",
        ip_address);
    for (int i = 0; i < std::ssize(PRIVATE_SUBNETS); ++i)
    {
        const auto & subnet = PRIVATE_SUBNETS[i];
        out += std::format(
            "length(tokens) = 1 and isIPAddressInRange(IPv4NumToString(ip), '{0}') or "
            "isIPAddressInRange(begin, '{0}') and isIPAddressInRange(end, '{0}'), true, ",
            subnet);
    }

    out += "false)";
    return true;
}

bool Ipv4NetmaskSuffix::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    ++pos;

    const auto ip_range = getConvertedArgument(function_name, pos);
    out = std::format(
        "multiIf(length(splitByChar('/', {0}) as tokens) > 2 or not isIPv4String(tokens[1]), null, "
        "length(tokens) = 1, 32, isNull(toUInt8OrNull(tokens[-1]) as suffix), null, toUInt8(min2(suffix, 32)))",
        ip_range);
    return true;
}

bool ParseIpv4::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toIPv4OrNull");
}

bool ParseIpv4Mask::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool Ipv6Compare::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool Ipv6IsMatch::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool ParseIpv6::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toIPv6OrNull");
}

bool ParseIpv6Mask::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool FormatIpv4::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool FormatIpv4Mask::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}
}
