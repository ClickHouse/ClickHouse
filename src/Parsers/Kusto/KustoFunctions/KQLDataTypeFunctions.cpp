#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
/*
#include <Parsers/Kusto/KustoFunctions/KQLDateTimeFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLStringFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLDynamicFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLAggregationFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLTimeSeriesFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLIPFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLBinaryFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLGeneralFunctions.h>
*/
#include <Parsers/Kusto/KustoFunctions/KQLDataTypeFunctions.h>
#include <format>

namespace DB
{

bool DatatypeBool::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool DatatypeDatetime::convertImpl(String &out,IParser::Pos &pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String datetime_str;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier)
        datetime_str = std::format("'{}'", String(pos->begin+1, pos->end -1));
    else if (pos->type == TokenType::StringLiteral)
        datetime_str = String(pos->begin, pos->end);
    else 
    {   auto start = pos;
        while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            ++pos;
            if  (pos->type == TokenType::ClosingRoundBracket)
                break;
        }
        --pos;
        datetime_str = std::format("'{}'",String(start->begin,pos->end));
    }
    out = std::format("toDateTime64({},9,'UTC')", datetime_str);
    ++pos;
    return true;
}

bool DatatypeDynamic::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool DatatypeGuid::convertImpl(String &out,IParser::Pos &pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String guid_str;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier)
        guid_str = std::format("'{}'", String(pos->begin+1, pos->end -1));
    else if (pos->type == TokenType::StringLiteral)
        guid_str = String(pos->begin, pos->end);
    else 
    {   auto start = pos;
        while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            ++pos;
            if  (pos->type == TokenType::ClosingRoundBracket)
                break;
        }
        --pos;
        guid_str = std::format("'{}'",String(start->begin,pos->end));
    }
    out = guid_str;
    ++pos;
    return true;
}

bool DatatypeInt::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool DatatypeLong::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool DatatypeReal::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool DatatypeString::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool DatatypeTimespan::convertImpl(String &out,IParser::Pos &pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;

    out = getConvertedArgument(fn_name, pos);
    return true;
}

bool DatatypeDecimal::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

}
