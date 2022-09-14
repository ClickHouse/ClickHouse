#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>

#include <format>
#include <Poco/String.h>
#include<regex>


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
    String arg;
     if (pos->type == TokenType::QuotedIdentifier)
        arg = String(pos->begin + 1, pos->end - 1);
    else if (pos->type == TokenType::StringLiteral)
        arg = String(pos->begin, pos->end);
    else
        arg = getConvertedArgument(function_name,pos);
    
    if(pos->type == TokenType::StringLiteral || pos->type == TokenType::QuotedIdentifier)
    {
        ++pos;
        try
        {
           auto result =  kqlCallToExpression("time", {arg}, pos.max_depth);
            out = std::format("{}" , result);
        }
        catch(...)
        {
            out = std::format("NULL");
        }
    }
    else
        out = std::format("{}" , arg);

    /*    
    if(pos->type == TokenType::StringLiteral || pos->type == TokenType::QuotedIdentifier)
    {
        --pos;
        String result;
        auto arg = getArgument(function_name,pos);
        try
        {
            result =  kqlCallToExpression("time", {arg}, pos.max_depth);
            out = std::format("{}" , result);
        }
        catch(...)
        {
            out = std::format("NULL");
        }

    }
    else
    {
        auto arg = getConvertedArgument(function_name,pos);
        out = std::format("{}" , arg);
    }
*/
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
    static const std::regex expr{"^[0-9]+e[+-]?[0-9]+"};
    bool is_string = std::any_of(res.begin(), res.end(), ::isalpha) && !(std::regex_match(res , expr));
     
    if (is_string)
        out = std::format("NULL");
    else if (std::regex_match(res , expr))
    {
        auto exponential_pos = res.find("e");
        if(res[exponential_pos +1] == '+' || res[exponential_pos +1] == '-' )
            scale = std::stoi(res.substr(exponential_pos+2 , res.length()));
        else
            scale = std::stoi(res.substr(exponential_pos+1,res.length()));

        out = std::format("toDecimal128({}::String,{})", res, scale);
    }
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
