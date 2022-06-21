#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>

namespace DB
{

bool Ipv4Compare::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Ipv4IsInRange::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Ipv4IsMatch::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Ipv4IsPrivate::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Ipv4NetmaskSuffix::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseIpv4::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseIpv4Mask::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Ipv6Compare::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool Ipv6IsMatch::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseIpv6::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ParseIpv6Mask::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool FormatIpv4::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool FormatIpv4Mask::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

}
