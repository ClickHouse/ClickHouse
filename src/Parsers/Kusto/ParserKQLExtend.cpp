#include <format>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLExtend.h>
#include <Parsers/Kusto/ParserKQLMakeSeries.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/ParserKQLProject.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>

namespace DB
{
bool ParserKQLExtend ::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_query;
    int32_t new_column_index = 1;

    String extend_expr = getExprFromToken(pos);

    String except_str;
    String new_extend_str;
    Tokens ntokens(extend_expr.data(), extend_expr.data() + extend_expr.size(), 0, true);
    IParser::Pos npos(ntokens, pos.max_depth, pos.max_backtracks);

    String alias;

    auto apply_alias = [&]
    {
        if (alias.empty())
        {
            alias = std::format("Column{}", new_column_index);
            ++new_column_index;
            new_extend_str += " AS";
        }
        else
            except_str = except_str.empty() ? " except " + alias : except_str + " except " + alias;

        new_extend_str = new_extend_str + " " + alias;

        alias.clear();
    };

    int32_t round_bracket_count = 0;
    int32_t square_bracket_count = 0;
    while (isValidKQLPos(npos))
    {
        if (npos->type == TokenType::OpeningRoundBracket)
            ++round_bracket_count;
        if (npos->type == TokenType::OpeningSquareBracket)
            ++square_bracket_count;
        if (npos->type == TokenType::ClosingRoundBracket)
            --round_bracket_count;
        if (npos->type == TokenType::ClosingSquareBracket)
            --square_bracket_count;

        auto expr = String(npos->begin, npos->end);
        if (expr == "AS")
        {
            ++npos;
            alias = String(npos->begin, npos->end);
        }

        if (npos->type == TokenType::Comma && square_bracket_count == 0 && round_bracket_count == 0)
        {
            apply_alias();
            new_extend_str += ", ";
        }
        else
            new_extend_str = new_extend_str.empty() ? expr : new_extend_str + " " + expr;

        ++npos;
    }
    apply_alias();

    String expr = std::format("SELECT * {}, {} from prev", except_str, new_extend_str);
    Tokens tokens(expr.data(), expr.data() + expr.size(), 0, true);
    IParser::Pos new_pos(tokens, pos.max_depth, pos.max_backtracks);

    if (!ParserSelectQuery().parse(new_pos, select_query, expected))
        return false;
    if (!setSubQuerySource(select_query, node, false, false))
        return false;

    node = select_query;
    return true;
}

}
