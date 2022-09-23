#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>

#include <format>
#include <Poco/String.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool ToBool::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format(
        "multiIf(toString({0}) = 'true', true, "
        "toString({0}) = 'false', false, toInt64OrNull(toString({0})) != 0)",
        param,
        generateUniqueIdentifier());
    return true;
}

bool ToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);

    out = std::format("parseDateTime64BestEffortOrNull(toString({0}),9,'UTC')", param);
    return true;
}

bool ToDouble::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format("toFloat64OrNull(toString({0}))", param);
    return true;
}

bool ToInt::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format("toInt32OrNull(toString({0}))", param);
    return true;
}

bool ToLong::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format("toInt64OrNull(toString({0}))", param);
    return true;
}

bool ToString::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = std::format("ifNull(toString({0}), '')", param);
    return true;
} 
bool ToTimeSpan::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;   
    ++pos;
    if(pos->type == TokenType::StringLiteral || pos->type == TokenType::QuotedIdentifier)
    {
        --pos;
        auto arg = getArgument(function_name,pos, ArgumentState::Raw);
        auto out1 =  kqlCallToExpression("time", {arg}, pos.max_depth);
        out = std::format("{}" , out1);
    }
    else
    {
        auto arg = getConvertedArgument(function_name,pos);
        out = std::format("{}" , arg);
    }

    return true;
}

bool ToDecimal::convertImpl(String & out, IParser::Pos & pos)
{

    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String res;
    int scale =0;
    int precision;
    
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
    {
        res =  String(pos->begin+1, pos->end -1);
        ++pos;
        precision = 34;
    }
    else
    {
        res = getConvertedArgument(fn_name, pos);
        precision = 17;
    }
    bool is_string = std::any_of(res.begin(), res.end(), ::isalpha) &&  Poco::toUpper(res) != "NULL";
    
    if ( Poco::toUpper(res) == "NULL")
        out = std::format("NULL");
    else if(is_string)
        throw Exception("Failed to parse String as decimal Literal: " + fn_name, ErrorCodes::BAD_ARGUMENTS);
    else
    {
       auto dot_pos = res.find(".");
        if(dot_pos != String::npos)
            scale = (precision - (res.substr(0,dot_pos-1)).length()) > 0 ? precision - (res.substr(0,dot_pos-1)).length() : 0;
        if(scale < 0)
            out = std::format("NULL");
        else
          out = std::format("toDecimal128({}::String,{})", res, scale);
    }

    return true;
}

}
