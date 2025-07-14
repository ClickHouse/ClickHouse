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

    const auto lhs = getArgument(function_name, pos, ArgumentState::Raw);
    const auto rhs = getArgument(function_name, pos, ArgumentState::Raw);
    const auto mask = getOptionalArgument(function_name, pos, ArgumentState::Parsed);
    out = std::format(
        "if(isNull({0} as lhs_ip_{5}) or isNull({1} as lhs_mask_{5}) "
        "or isNull({2} as rhs_ip_{5}) or isNull({3} as rhs_mask_{5}), null, "
        "sign(IPv4StringToNumOrNull(toString((tupleElement(IPv4CIDRToRange(assumeNotNull(lhs_ip_{5}), "
        "toUInt8(min2({4}, min2(assumeNotNull(lhs_mask_{5}), assumeNotNull(rhs_mask_{5})))) as mask_{5}), 1))))"
        "   - IPv4StringToNumOrNull(toString((tupleElement(IPv4CIDRToRange(assumeNotNull(rhs_ip_{5}), mask_{5}), 1))))))",
        kqlCallToExpression("parse_ipv4", {lhs}, pos.max_depth, pos.max_backtracks),
        kqlCallToExpression("ipv4_netmask_suffix", {lhs}, pos.max_depth, pos.max_backtracks),
        kqlCallToExpression("parse_ipv4", {rhs}, pos.max_depth, pos.max_backtracks),
        kqlCallToExpression("ipv4_netmask_suffix", {rhs}, pos.max_depth, pos.max_backtracks),
        mask ? *mask : "32",
        generateUniqueIdentifier());
    return true;
}

bool Ipv4IsInRange::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos, ArgumentState::Parsed);
    const auto ip_range = getArgument(function_name, pos, ArgumentState::Raw);
    out = std::format(
        "if(isNull(IPv4StringToNumOrNull({0}) as ip_{3}) "
        "or isNull({1} as range_start_ip_{3}) or isNull({2} as range_mask_{3}), null, "
        "bitXor(range_start_ip_{3}, bitAnd(ip_{3}, bitNot(toUInt32(intExp2(toInt32(32 - range_mask_{3})) - 1)))) = 0) ",
        ip_address,
        kqlCallToExpression("parse_ipv4", {ip_range}, pos.max_depth, pos.max_backtracks),
        kqlCallToExpression("ipv4_netmask_suffix", {ip_range}, pos.max_depth, pos.max_backtracks),
        generateUniqueIdentifier());
    return true;
}

bool Ipv4IsMatch::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos, ArgumentState::Raw);
    const auto rhs = getArgument(function_name, pos, ArgumentState::Raw);
    const auto mask = getOptionalArgument(function_name, pos, ArgumentState::Raw);
    out = std::format("equals({}, 0)", kqlCallToExpression("ipv4_compare", {lhs, rhs, mask ? *mask : "32"}, pos.max_depth, pos.max_backtracks));
    return true;
}

bool Ipv4IsPrivate::convertImpl(String & out, IParser::Pos & pos)
{
    static const std::array<String, 3> s_private_subnets{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"};

    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos);
    const auto unique_identifier = generateUniqueIdentifier();

    out += std::format(
        "multiIf(length(splitByChar('/', {0}) as tokens_{1}) > 2 or isNull(toIPv4OrNull(tokens_{1}[1]) as nullable_ip_{1}) "
        "or length(tokens_{1}) = 2 and isNull(toUInt8OrNull(tokens_{1}[-1]) as mask_{1}), null, "
        "ignore(assumeNotNull(nullable_ip_{1}) as ip_{1}, "
        "IPv4CIDRToRange(ip_{1}, assumeNotNull(mask_{1})) as range_{1}, IPv4NumToString(tupleElement(range_{1}, 1)) as begin_{1}, "
        "IPv4NumToString(tupleElement(range_{1}, 2)) as end_{1}), null, ",
        ip_address,
        unique_identifier);
    for (int i = 0; i < std::ssize(s_private_subnets); ++i)
    {
        if (i > 0)
            out += " or ";

        const auto & subnet = s_private_subnets[i];
        out += std::format(
            "length(tokens_{1}) = 1 and isIPAddressInRange(IPv4NumToString(ip_{1}), '{0}') or "
            "length(tokens_{1}) = 2 and isIPAddressInRange(begin_{1}, '{0}') and isIPAddressInRange(end_{1}, '{0}')",
            subnet,
            unique_identifier);
    }

    out.push_back(')');
    return true;
}

bool Ipv4NetmaskSuffix::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_range = getArgument(function_name, pos);
    out = std::format(
        "multiIf(length(splitByChar('/', {0}) as tokens_{1}) > 2 or not isIPv4String(tokens_{1}[1]), null, "
        "length(tokens_{1}) = 1, 32, isNull(toUInt8OrNull(tokens_{1}[-1]) as mask_{1}), null, toUInt8(min2(mask_{1}, 32)))",
        ip_range,
        generateUniqueIdentifier());
    return true;
}

bool ParseIpv4::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos);
    out = std::format(
        "multiIf(length(splitByChar('/', {0}) as tokens_{1}) = 1, IPv4StringToNumOrNull(tokens_{1}[1]) as ip_{1}, "
        "length(tokens_{1}) = 2 and isNotNull(ip_{1}) and isNotNull(toUInt8OrNull(tokens_{1}[-1]) as mask_{1}), "
        "IPv4StringToNumOrNull(toString(tupleElement(IPv4CIDRToRange(toIPv4(assumeNotNull(ip_{1})), assumeNotNull(mask_{1})), 1))), null)",
        ip_address,
        generateUniqueIdentifier());
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
        "if(isNull(toIPv4OrNull({0}) as ip_{2}) or isNull(toUInt8OrNull(toString({1})) as mask_{2}), null, "
        "toUInt32(tupleElement(IPv4CIDRToRange(assumeNotNull(ip_{2}), arrayMax([0, arrayMin([32, assumeNotNull(mask_{2})])])), 1)))",
        ip_address,
        mask,
        generateUniqueIdentifier());
    return true;
}

bool Ipv6Compare::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos);
    const auto rhs = getArgument(function_name, pos);
    const auto mask = getOptionalArgument(function_name, pos);
    const auto calculated_mask = mask ? *mask : "128";
    out = std::format(
        "if(length(splitByChar('/', {1}) as lhs_tokens_{0}) > 2 or length(splitByChar('/', {2}) as rhs_tokens_{0}) > 2 "
        "or isNull(IPv6StringToNumOrNull(lhs_tokens_{0}[1]) as lhs_ipv6_{0}) or length(lhs_tokens_{0}) = 2 "
        "and isNull((if(isIPv4String(lhs_tokens_{0}[1]), 96, 0) + toUInt8OrNull(lhs_tokens_{0}[-1])) as lhs_suffix_{0}) "
        "or isNull(IPv6StringToNumOrNull(rhs_tokens_{0}[1]) as rhs_ipv6_{0}) or length(rhs_tokens_{0}) = 2 "
        "and isNull((if(isIPv4String(rhs_tokens_{0}[1]), 96, 0) + toUInt8OrNull(rhs_tokens_{0}[-1])) as rhs_suffix_{0}) "
        "or isNull(toUInt8(min2({3}, min2(ifNull(lhs_suffix_{0}, 128), ifNull(rhs_suffix_{0}, 128)))) as suffix_{0}) "
        "or isNull(bitShiftLeft(bitShiftRight(bitNot(reinterpretAsFixedString(0::UInt128)), (128 - suffix_{0}) as zeroes_{0}), "
        "zeroes_{0}) as mask_{0}) or isNull(bitAnd(lhs_ipv6_{0}, mask_{0}) as lhs_base_{0}) "
        "or isNull(bitAnd(rhs_ipv6_{0}, mask_{0}) as rhs_base_{0}), null, "
        "multiIf(lhs_base_{0} < rhs_base_{0}, -1, lhs_base_{0} > rhs_base_{0}, 1, 0))",
        generateUniqueIdentifier(),
        lhs,
        rhs,
        calculated_mask);
    return true;
}

bool Ipv6IsMatch::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto lhs = getArgument(function_name, pos, ArgumentState::Raw);
    const auto rhs = getArgument(function_name, pos, ArgumentState::Raw);
    const auto mask = getOptionalArgument(function_name, pos, ArgumentState::Raw);
    out = std::format("equals({}, 0)", kqlCallToExpression("ipv6_compare", {lhs, rhs, mask ? *mask : "128"}, pos.max_depth, pos.max_backtracks));
    return true;
}

bool ParseIpv6::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos);
    out = std::format(
        "if(length(splitByChar('/', assumeNotNull({0})) as tokens_{1}) > 2 or isNull(IPv6StringToNumOrNull(tokens_{1}[1]) as ip_{1}) "
        "or length(tokens_{1}) = 2 and isNull(toUInt8OrNull(tokens_{1}[-1]) as mask_{1}), null, "
        "arrayStringConcat(flatten(extractAllGroups(lower(hex(tupleElement(IPv6CIDRToRange(assumeNotNull(ip_{1}), toUInt8(ifNull(mask_{1} "
        "+ if(isIPv4String(tokens_{1}[1]), 96, 0), 128))), 1))), '([\\da-f]{{4}})')), ':'))",
        ip_address,
        generateUniqueIdentifier());
    return true;
}

bool ParseIpv6Mask::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos, ArgumentState::Raw);
    const auto mask = getArgument(function_name, pos, ArgumentState::Raw);
    const auto unique_identifier = generateUniqueIdentifier();
    out = std::format(
        "if(empty({0} as ipv4_{3}), {1}, {2})",
        kqlCallToExpression("format_ipv4", {"trim_start('::', " + ip_address + ")", mask + " - 96"}, pos.max_depth, pos.max_backtracks),
        kqlCallToExpression("parse_ipv6", {"strcat(tostring(parse_ipv6(" + ip_address + ")), '/', tostring(" + mask + "))"}, pos.max_depth, pos.max_backtracks),
        kqlCallToExpression("parse_ipv6", {"ipv4_" + unique_identifier}, pos.max_depth, pos.max_backtracks),
        unique_identifier);
    return true;
}

bool FormatIpv4::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos, ArgumentState::Raw);
    const auto mask = getOptionalArgument(function_name, pos, ArgumentState::Parsed);
    out = std::format(
        "ifNull(if(isNotNull(toUInt32OrNull(toString({0})) as param_as_uint32_{3}) and toTypeName({0}) = 'String' or ({1}) < 0 "
        "or isNull(ifNull(param_as_uint32_{3}, {2}) as ip_as_number_{3}), null, "
        "IPv4NumToString(bitAnd(ip_as_number_{3}, bitNot(toUInt32(intExp2(toInt32(32 - ({1}))) - 1))))), '')",
        ParserKQLBase::getExprFromToken(ip_address, pos.max_depth, pos.max_backtracks),
        mask ? *mask : "32",
        kqlCallToExpression("parse_ipv4", {"tostring(" + ip_address + ")"}, pos.max_depth, pos.max_backtracks),
        generateUniqueIdentifier());
    return true;
}

bool FormatIpv4Mask::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto ip_address = getArgument(function_name, pos, ArgumentState::Raw);
    const auto mask = getOptionalArgument(function_name, pos, ArgumentState::Raw);
    const auto calculated_mask = mask ? *mask : "32";
    out = std::format(
        "if(empty({1} as formatted_ip_{2}) or position(toTypeName({0}), 'Int') = 0 or not {0} between 0 and 32, '', "
        "concat(formatted_ip_{2}, '/', toString(toInt64(min2({0}, ifNull({3} as suffix_{2}, 32))))))",
        ParserKQLBase::getExprFromToken(calculated_mask, pos.max_depth, pos.max_backtracks),
        kqlCallToExpression("format_ipv4", {ip_address, calculated_mask}, pos.max_depth, pos.max_backtracks),
        generateUniqueIdentifier(),
        kqlCallToExpression("ipv4_netmask_suffix", {"tostring(" + ip_address + ")"}, pos.max_depth, pos.max_backtracks));
    return true;
}
}
