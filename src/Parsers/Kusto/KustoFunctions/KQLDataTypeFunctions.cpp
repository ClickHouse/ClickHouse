#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLDataTypeFunctions.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>
#include <format>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool DatatypeBool::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out, pos, "toBool");
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
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    String array;
    ++pos;
    if (pos->type == TokenType::OpeningSquareBracket) 
    {   
        ++pos;
        while (pos->type != TokenType::ClosingRoundBracket)
        {
            auto tmp_arg = getConvertedArgument(fn_name, pos);
            array = array.empty() ? tmp_arg : array +", " + tmp_arg;
            ++pos;
        }
        out = "array (" + array + ")";
        return true;
    }
    else
        return false; // should throw exception , later
}

bool DatatypeGuid::convertImpl(String &out,IParser::Pos &pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String guid_str;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        guid_str = String(pos->begin+1, pos->end -1);
    else 
    {   auto start = pos;
        while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            ++pos;
            if  (pos->type == TokenType::ClosingRoundBracket)
                break;
        }
        --pos;
        guid_str = String(start->begin,pos->end);
    }
    out = std::format("toUUID('{}')", guid_str);
    ++pos;
    return true;
}

bool DatatypeInt::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out, pos, "toInt32");
}

bool DatatypeLong::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out, pos, "toInt64");
}

bool DatatypeReal::convertImpl(String &out,IParser::Pos &pos)
{
    return directMapping(out, pos, "toFloat64");
}

bool DatatypeString::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

bool DatatypeTimespan::convertImpl(String &out,IParser::Pos &pos)
{
    ParserKQLDateTypeTimespan time_span;
    ASTPtr node;
    Expected expected;

    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;

    if (time_span.parse(pos, node, expected))
    {
        out = std::to_string(time_span.toSeconds());
        ++pos;
    }
    else
        throw Exception("Not a correct timespan expression: " + fn_name, ErrorCodes::BAD_ARGUMENTS);
    return true;
}

bool DatatypeDecimal::convertImpl(String &out,IParser::Pos &pos)
{
    String res = String(pos->begin,pos->end);
    out = res;
    return false;
}

}
