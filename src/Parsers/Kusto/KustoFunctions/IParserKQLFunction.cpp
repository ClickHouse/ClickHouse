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
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>
#include <Parsers/Kusto/ParserKQLOperators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool IParserKQLFunction::convert(String &out,IParser::Pos &pos)
{
    return wrapConvertImpl(pos, IncreaseDepthTag{}, [&]
    {
        bool res = convertImpl(out,pos);
        if (!res)
            out = "";
        return res;
    });
}

bool IParserKQLFunction::directMapping(String &out,IParser::Pos &pos,const String &ch_fn)
{
    std::vector<String> arguments;

    String fn_name = getKQLFunctionName(pos); 

    if (fn_name.empty())
        return false;

    String res;
    auto begin = pos;
    ++pos;
    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        String argument = getConvertedArgument(fn_name,pos);
        arguments.push_back(argument);

        if (pos->type == TokenType::ClosingRoundBracket)
        {
            for (auto arg : arguments) 
            {
                if (res.empty())
                    res = ch_fn + "(" + arg;
                else
                    res = res + ", "+ arg;
            }
            res += ")";

            out = res;
            return true;
        }
        ++pos;
    }

    pos = begin;
    return false;
}

String IParserKQLFunction::getConvertedArgument(const String &fn_name, IParser::Pos &pos)
{
    String converted_arg;
    std::vector<String> tokens;
    std::unique_ptr<IParserKQLFunction> fun;

    if (pos->type == TokenType::ClosingRoundBracket)
        return converted_arg;

    if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        throw Exception("Syntax error near " + fn_name, ErrorCodes::SYNTAX_ERROR);

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        String token = String(pos->begin,pos->end);
        String new_token;
        if (!KQLOperators().convert(tokens,pos))
        {
            if (pos->type == TokenType::BareWord )
            {
                String converted;
                fun = KQLFunctionFactory::get(token);
                if ( fun && fun->convert(converted,pos))
                    tokens.push_back(converted);
                else
                    tokens.push_back(token);
            }
            else if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
            {
                break;
            }
            else
                tokens.push_back(token);
        }
        ++pos;
        if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
            break;
    }
    for (auto token : tokens) 
        converted_arg = converted_arg + token +" ";

    return converted_arg;
}

String IParserKQLFunction::getKQLFunctionName(IParser::Pos &pos)
{
    String fn_name = String(pos->begin, pos->end);
    ++pos;
    if (pos->type != TokenType::OpeningRoundBracket)
    {
        --pos;
        return "";
    }
    return  fn_name;
}

}
