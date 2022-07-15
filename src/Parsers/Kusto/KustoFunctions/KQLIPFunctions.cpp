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

namespace
{
String trimQuotes(const String & str)
{
    static constexpr auto QUOTE = '\'';

    const auto first_index = str.find(QUOTE);
    const auto last_index = str.rfind(QUOTE);
    if (first_index == String::npos || last_index == String::npos)
        throw DB::Exception("Syntax error, improper quotation: " + str, DB::ErrorCodes::SYNTAX_ERROR);

    return str.substr(first_index + 1, last_index - first_index - 1);
}
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
    const auto slash_index = ip_range.find('/');
    out = std::format(slash_index == String::npos ? "{0} = {1}" : "isIPAddressInRange({0}, {1})", ip_address, ip_range);
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

    const auto ip_address = trimQuotes(getConvertedArgument(function_name, pos));
    const auto slash_index = ip_address.find('/');

    out += "or(";
    for (int i = 0; i < std::ssize(PRIVATE_SUBNETS); ++i)
    {
        out += i > 0 ? ", " : "";

        const auto & subnet = PRIVATE_SUBNETS[i];
        out += slash_index == String::npos
            ? std::format("isIPAddressInRange('{0}', '{1}')", ip_address, subnet)
            : std::format(
                "and(isIPAddressInRange(IPv4NumToString(tupleElement((IPv4CIDRToRange(toIPv4('{0}'), {1}) as range), 1)) as begin, '{2}'), "
                "isIPAddressInRange(IPv4NumToString(tupleElement(range, 2)) as end, '{2}'))",
                std::string_view(ip_address.c_str(), slash_index),
                std::string_view(ip_address.c_str() + slash_index + 1, ip_address.length() - slash_index - 1),
                subnet);
    }

    out += ")";
    return true;
}

bool Ipv4NetmaskSuffix::convertImpl(String & out, IParser::Pos & pos)
{
    static constexpr auto DEFAULT_NETMASK = 32;

    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    ++pos;

    const auto ip_range = trimQuotes(getConvertedArgument(function_name, pos));
    const auto slash_index = ip_range.find('/');
    const std::string_view ip_address(ip_range.c_str(), std::min(ip_range.length(), slash_index));
    const auto netmask = slash_index == String::npos ? DEFAULT_NETMASK : std::strtol(ip_range.c_str() + slash_index + 1, nullptr, 10);
    out = std::format("if(and(isIPv4String('{0}'), {1} between 1 and 32), {1}, null)", ip_address, netmask);
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
