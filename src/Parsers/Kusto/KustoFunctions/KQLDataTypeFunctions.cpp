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
#include<regex>

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
        datetime_str = std::format("{}::String", String(start->begin, pos->end));
    }
    out = std::format("parseDateTime64BestEffortOrNull({},9,'UTC')", datetime_str);
    ++pos;
    return true;
}

bool DatatypeDynamic::convertImpl(String & out, IParser::Pos & pos)
{
    static const std::unordered_set<std::string_view> ALLOWED_FUNCTIONS{"date", "datetime", "dynamic", "time", "timespan"};

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
            if (const std::string_view token(pos->begin, pos->end); token_type == TokenType::BareWord && !ALLOWED_FUNCTIONS.contains(token))
            {
                ++pos;
                if (pos->type != TokenType::ClosingRoundBracket && pos->type != TokenType::ClosingSquareBracket
                    && pos->type != TokenType::Comma)
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Expression {} is not supported inside {}", token, function_name);

                --pos;
            }

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
    out = std::format("toUUIDOrNull('{}')", guid_str);
    ++pos;
    return true;
}

bool DatatypeInt::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    String guid_str;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        throw Exception("String is not parsed as int literal.", ErrorCodes::BAD_ARGUMENTS);
    else
    {
        auto arg = getConvertedArgument(fn_name, pos);
        out = std::format("toInt32({})", arg);
    }
    return true;
}

bool DatatypeLong::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "toInt64");
}

bool DatatypeReal::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        throw Exception("String is not parsed as double literal.", ErrorCodes::BAD_ARGUMENTS);
    else
    {
        auto arg = getConvertedArgument(fn_name, pos);
        out = std::format("toFloat64({})", arg);
    }
    return true;
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
    bool sign = false;

    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    ++pos;
    if (pos->type == TokenType::Minus)
    {
        sign = true;
        ++pos;
    }
    if (time_span.parse(pos, node, expected))
    {
        if (sign)
            out = std::format("-{}::Float64", time_span.toSeconds());
        else
            out = std::format("{}::Float64", time_span.toSeconds());
        ++pos;
    }
    else
        throw Exception("Not a correct timespan expression: " + fn_name, ErrorCodes::BAD_ARGUMENTS);
    return true;
}

bool DatatypeDecimal::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String arg;
    int scale = 0, length = 0;
    int precision = 34;

    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
        throw Exception("Failed to parse String as decimal Literal: " + fn_name, ErrorCodes::BAD_ARGUMENTS);

    --pos;
    arg = getArgument(fn_name, pos);

    //NULL expr returns NULL not execption
     static const std::regex expr{"^[0-9]+e[+-]?[0-9]+"};
    bool is_string = std::any_of(arg.begin(), arg.end(), ::isalpha) && Poco::toUpper(arg) != "NULL" && !(std::regex_match(arg , expr));
    if (is_string)
        throw Exception("Failed to parse String as decimal Literal: " + fn_name, ErrorCodes::BAD_ARGUMENTS);
    
    if (std::regex_match(arg , expr))
    {
        auto exponential_pos = arg.find("e");
        if(arg[exponential_pos +1] == '+' || arg[exponential_pos +1] == '-' )
            scale = std::stoi(arg.substr(exponential_pos+2,arg.length()));
        else
            scale = std::stoi(arg.substr(exponential_pos+1 , arg.length()));

        out = std::format("toDecimal128({}::String,{})", arg, scale);
        return true;
    }

    auto dot_pos = arg.find(".");
    if (dot_pos != String::npos)
    {
        length = arg.substr(0, dot_pos - 1).length();
        scale = (precision - length) > 0 ? precision - length : 0;
    }
    if (is_string)
        throw Exception("Failed to parse String as decimal Literal: " + fn_name, ErrorCodes::BAD_ARGUMENTS);

    if (scale < 0 || Poco::toUpper(arg) == "NULL")
        out = "NULL";
    else
        out = std::format("toDecimal128({}::String,{})", arg, scale);

    return true;
}

}
