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

bool BinaryAnd::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BinaryNot::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BinaryOr::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BinaryShiftLeft::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BinaryShiftRight::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BinaryXor::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool BitsetCountOnes::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

}
