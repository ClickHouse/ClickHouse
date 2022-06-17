
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>

namespace DB
{
bool Tobool::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ToDatetime::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ToDouble::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ToInt::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ToString::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool ToTimespan::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

}
