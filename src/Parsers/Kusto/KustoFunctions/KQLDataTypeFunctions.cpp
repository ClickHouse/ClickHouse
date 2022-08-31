#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLDataTypeFunctions.h>
#include <Parsers/Kusto/ParserKQLDateTypeTimespan.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/ParserSetQuery.h>

#include <format>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYNTAX_ERROR;
}

bool DatatypeBool::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toBool");
}

bool DatatypeDatetime::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String datetime_str;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier)
        datetime_str = std::format("'{}'", String(pos->begin + 1, pos->end - 1));
    else if (pos->type == TokenType::StringLiteral)
        datetime_str = String(pos->begin, pos->end);
    else
    {
        auto start = pos;
        while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            ++pos;
            if (pos->type == TokenType::ClosingRoundBracket)
                break;
        }
        --pos;
        datetime_str = std::format("'{}'", String(start->begin, pos->end));
    }
    out = std::format("parseDateTime64BestEffortOrNull({},9,'UTC')", datetime_str);
    ++pos;
    return true;
}

bool DatatypeDynamic::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    ++pos;
    if (pos->type == TokenType::OpeningCurlyBrace)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property bags are not supported for now in {}", function_name);

    while (!pos->isEnd() && pos->type != TokenType::ClosingRoundBracket)
    {
        if (const auto token_type = pos->type; token_type == TokenType::BareWord || token_type == TokenType::Number
            || token_type == TokenType::QuotedIdentifier || token_type == TokenType::StringLiteral)
        {
            if (const std::string_view token(pos->begin, pos->end);
                token_type == TokenType::BareWord && token != "dynamic" && token != "datetime" && token != "timespan" && token != "null")
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Expression {} is not supported inside {}", token, function_name);

            out.append(getConvertedArgument(function_name, pos));
        }
        else
        {
            out.append(pos->begin, pos->end);
            ++pos;
        }
    }

    return true;
}

bool DatatypeGuid::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String guid_str;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        guid_str = String(pos->begin + 1, pos->end - 1);
    else
    {
        auto start = pos;
        while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
        {
            ++pos;
            if (pos->type == TokenType::ClosingRoundBracket)
                break;
        }
        --pos;
        guid_str = String(start->begin, pos->end);
    }
    out = std::format("toUUID('{}')", guid_str);
    ++pos;
    return true;
}

bool DatatypeInt::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toInt32");
}

bool DatatypeLong::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toInt64");
}

bool DatatypeReal::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toFloat64");
}

bool DatatypeString::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

bool DatatypeTimespan::convertImpl(String & out, IParser::Pos & pos)
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

bool DatatypeDecimal::convertImpl(String & out, IParser::Pos & pos)
{
    String res = String(pos->begin, pos->end);
    out = res;
    return false;
}

}
