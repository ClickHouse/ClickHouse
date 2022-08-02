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

namespace DB
{
bool Ipv4Compare::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos);
    const auto rhs = getArgument(function_name, pos);
    const auto mask = getOptionalArgument(function_name, pos);

    out = std::format(
        "multiIf(length(splitByChar('/', {0}) as lhs) > 2 or length(splitByChar('/', {1}) as rhs) > 2, null, "
        "isNull(toIPv4OrNull(lhs[1]) as lhs_ip) or length(lhs) = 2 and isNull(toUInt8OrNull(lhs[-1]) as lhs_mask) or "
        "isNull(toIPv4OrNull(rhs[1]) as rhs_ip) or length(rhs) = 2 and isNull(toUInt8OrNull(rhs[-1]) as rhs_mask), null, "
        "ignore(toUInt8(min2(32, min2({2}, min2(ifNull(lhs_mask, 32), ifNull(rhs_mask, 32))))) as mask), null, "
        "sign(toInt32(tupleElement(IPv4CIDRToRange(assumeNotNull(lhs_ip), mask), 1))"
        "   - toInt32(tupleElement(IPv4CIDRToRange(assumeNotNull(rhs_ip), mask), 1))))",
        lhs,
        rhs,
        mask ? *mask : "32");
    return true;
}

bool Ipv4IsInRange::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos);
    const auto ip_range = getArgument(function_name, pos);
    out = std::format(
        "if(isNull(IPv4StringToNumOrNull({0}) as ip) or isNull({2} as calculated_mask) or "
        "isNull(toIPv4OrNull(tokens[1]) as range_prefix_ip), null, isIPAddressInRange(IPv4NumToString(assumeNotNull(ip)), "
        "concat(IPv4NumToString(assumeNotNull(range_prefix_ip)), '/', toString(assumeNotNull(calculated_mask)))))",
        ip_address,
        ip_range,
        kqlCallToExpression("ipv4_netmask_suffix", {ip_range}, pos.max_depth));
    return true;
}

bool Ipv4IsMatch::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos);
    const auto rhs = getArgument(function_name, pos);
    const auto mask = getOptionalArgument(function_name, pos);

    out = std::format("{} = 0", kqlCallToExpression("ipv4_compare", {lhs, rhs, mask ? *mask : "32"}, pos.max_depth));
    return true;
}

bool Ipv4IsPrivate::convertImpl(String & out, IParser::Pos & pos)
{
    static const std::array<String, 3> s_private_subnets{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"};

    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos);

    out += std::format(
        "multiIf(length(splitByChar('/', {0}) as tokens) > 2 or isNull(toIPv4OrNull(tokens[1]) as nullable_ip), null, "
        "length(tokens) = 2 and isNull(toUInt8OrNull(tokens[-1]) as mask), null, "
        "ignore(assumeNotNull(nullable_ip) as ip, "
        "IPv4CIDRToRange(ip, assumeNotNull(mask)) as range, IPv4NumToString(tupleElement(range, 1)) as begin, "
        "IPv4NumToString(tupleElement(range, 2)) as end), null, ",
        ip_address);
    for (int i = 0; i < std::ssize(s_private_subnets); ++i)
    {
        const auto & subnet = s_private_subnets[i];
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

    const auto ip_range = getArgument(function_name, pos);
    out = std::format(
        "multiIf(length(splitByChar('/', {0}) as tokens) > 2 or not isIPv4String(tokens[1]), null, "
        "length(tokens) = 1, 32, isNull(toUInt8OrNull(tokens[-1]) as mask), null, toUInt8(min2(mask, 32)))",
        ip_range);
    return true;
}

bool ParseIpv4::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos);
    out = std::format(
        "multiIf(length(splitByChar('/', {0}) as tokens) = 1, IPv4StringToNumOrNull(tokens[1]) as ip, "
        "length(tokens) = 2 and isNotNull(ip) and isNotNull(toUInt8OrNull(tokens[-1]) as mask), "
        "tupleElement(IPv4CIDRToRange(assumeNotNull(ip), assumeNotNull(mask)), 1), null)",
        ip_address);
    return true;
}

bool ParseIpv4Mask::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos);
    const auto mask = getArgument(function_name, pos);
    out = std::format(
        "if(isNull(toIPv4OrNull({0}) as ip) or isNull(toUInt8OrNull(toString({1})) as mask), null, "
        "toUInt32(tupleElement(IPv4CIDRToRange(assumeNotNull(ip), toUInt8(max2(0, min2(32, assumeNotNull(mask))))), 1)))",
        ip_address,
        mask);
    return true;
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
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos);
    out = std::format(
        "if(isNull(ifNull(if(isNull({1} as ipv4), null, IPv4ToIPv6(ipv4)), IPv6StringToNumOrNull({0})) as ipv6), null, "
        "arrayStringConcat(flatten(extractAllGroups(lower(hex(assumeNotNull(ipv6))), '([\\da-f]{{4}})')), ':'))",
        ip_address,
        kqlCallToExpression("parse_ipv4", {ip_address}, pos.max_depth));
    return true;
}

bool ParseIpv6Mask::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool FormatIpv4::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos);
    const auto mask = getOptionalArgument(function_name, pos);
    out = std::format(
        "ifNull(multiIf(isNotNull(toUInt32OrNull(toString({0})) as param_as_uint32) and toTypeName({0}) = 'String' or {1} < 0, null, "
        "isNull(ifNull(param_as_uint32, {2}) as ip_as_number), null, "
        "IPv4NumToString(bitAnd(ip_as_number, bitNot(toUInt32(intExp2(32 - {1}) - 1))))), '')",
        ip_address,
        mask ? *mask : "32",
        kqlCallToExpression("parse_ipv4", {"tostring(" + ip_address + ")"}, pos.max_depth));
    return true;
}

bool FormatIpv4Mask::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos);
    const auto mask = getOptionalArgument(function_name, pos);
    const auto calculated_mask = mask ? *mask : "32";
    out = std::format(
        "if(empty({1} as formatted_ip) or not {0} between 0 and 32, '', concat(formatted_ip, '/', toString({0})))",
        calculated_mask,
        kqlCallToExpression("format_ipv4", {ip_address, calculated_mask}, pos.max_depth));
    return true;
}
}
