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
    std::unique_ptr<IParserKQLFunction> fun;
    std::vector<String> args;

    String res =ch_fn + "(";
    out = res;
    auto begin = pos;

    ++pos;
    if (pos->type != TokenType::OpeningRoundBracket)
    {
        pos = begin;
        return false;
    }

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        ++pos;
        String tmp_arg = String(pos->begin,pos->end);
        if (pos->type == TokenType::BareWord )
        {
            String new_arg;
            fun = KQLFunctionFactory::get(tmp_arg);
            if (fun && fun->convert(new_arg,pos))
                tmp_arg = new_arg;
        }
        else if (pos->type == TokenType::ClosingRoundBracket)
        {
            for (auto arg : args)
                res+=arg;

            res += ")";
            out = res;
            return true;
        }
        args.push_back(tmp_arg);
    }

    pos = begin;
    return false;
}

String IParserKQLFunction::getConvertedArgument(const String &fn_name, IParser::Pos &pos)
{
    String converted_arg;
    std::unique_ptr<IParserKQLFunction> fun;

    if (pos->type == TokenType::ClosingRoundBracket)
        return converted_arg;

    if (pos->isEnd() || pos->type == TokenType::PipeMark || pos->type == TokenType::Semicolon)
        throw Exception("Syntax error near " + fn_name, ErrorCodes::SYNTAX_ERROR);

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        String token = String(pos->begin,pos->end);
        if (pos->type == TokenType::BareWord )
        {
            String converted;
            fun = KQLFunctionFactory::get(token);
            if ( fun && fun->convert(converted,pos))
                converted_arg += converted;
            else
                converted_arg += token;
        }
        else if (pos->type == TokenType::Comma ||pos->type == TokenType::ClosingRoundBracket) 
        {
            break;
        }
        else
            converted_arg += token;
        ++pos;
    }
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
