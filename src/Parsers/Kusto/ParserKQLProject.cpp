#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLProject.h>
namespace DB
{

bool ParserKQLProject :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto begin = pos;
    String expr;
    if (op_pos.empty())
        expr = "*";
    else
    {
        for (auto it = op_pos.begin(); it != op_pos.end(); ++it)
        {
            pos = *it ;
            while (!pos->isEnd() && pos->type != TokenType::PipeMark)
            {
                if (pos->type == TokenType::BareWord)
                {
                    String tmp(pos->begin,pos->end);

                    if (it != op_pos.begin() && columns.find(tmp) == columns.end())
                        return false;
                    columns.insert(tmp);
                }
                ++pos;
            }
        }
        expr = getExprFromToken(op_pos.back());
    }

    Tokens tokens(expr.c_str(), expr.c_str()+expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(new_pos, node, expected))
        return false;

    pos = begin;

    return true;
}

}
