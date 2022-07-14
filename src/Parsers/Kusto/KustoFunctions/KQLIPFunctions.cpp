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
    static constexpr auto sQuote = '\'';

    const auto firstIndex = str.find(sQuote);
    const auto lastIndex = str.rfind(sQuote);
    if (firstIndex == String::npos || lastIndex == String::npos)
        throw DB::Exception("Syntax error, improper quotation: " + str, DB::ErrorCodes::SYNTAX_ERROR);

    return str.substr(firstIndex + 1, lastIndex - firstIndex - 1);
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
    const auto functionName = getKQLFunctionName(pos);
    ++pos;

    const auto ipAddress = getConvertedArgument(functionName, pos);
    ++pos;

    const auto ipRange = getConvertedArgument(functionName, pos);
    const auto slashIndex = ipRange.find('/');
    out = std::format(slashIndex == String::npos ? "{0} = {1}" : "isIPAddressInRange({0}, {1})", ipAddress, ipRange);
    return true;
}

bool Ipv4IsMatch::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool Ipv4IsPrivate::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Ipv4NetmaskSuffix::convertImpl(String & out, IParser::Pos & pos)
{
    static constexpr auto sDefaultNetmask = 32;

    const auto functionName = getKQLFunctionName(pos);
    ++pos;

    const auto ipRange = trimQuotes(getConvertedArgument(functionName, pos));
    const auto slashIndex = ipRange.find('/');
    const auto ipAddress = ipRange.substr(0, slashIndex);
    const auto netmask = slashIndex == String::npos ? sDefaultNetmask : std::strtol(ipRange.c_str() + slashIndex + 1, nullptr, 10);
    out = std::format("if(and(isIPv4String('{0}'), {1} between 1 and 32), {1}, null)", ipAddress, netmask);
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
