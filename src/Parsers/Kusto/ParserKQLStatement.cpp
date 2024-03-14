#include <Parsers/IParserBase.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

bool ParserKQLStatement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKQLWithOutput query_with_output_p(end, allow_settings_after_format_in_insert);
    ParserSetQuery set_p;

    bool res = query_with_output_p.parse(pos, node, expected)
        || set_p.parse(pos, node, expected);

    return res;
}

bool ParserKQLWithOutput::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKQLWithUnionQuery kql_p;

    ASTPtr query;
    bool parsed = kql_p.parse(pos, query, expected);

    if (!parsed)
        return false;

    node = std::move(query);
    return true;
}

bool ParserKQLWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr kql_query;

    if (!ParserKQLQuery().parse(pos, kql_query, expected))
        return false;

    if (kql_query->as<ASTSelectWithUnionQuery>())
    {
        node = std::move(kql_query);
        return true;
    }

    auto list_node = std::make_shared<ASTExpressionList>();
    list_node->children.push_back(kql_query);

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    node = select_with_union_query;
    select_with_union_query->list_of_selects = list_node;
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);

    return true;
}

bool ParserKQLTaleFunction::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKQLWithUnionQuery kql_p;
    ASTPtr select;
    ParserToken s_lparen(TokenType::OpeningRoundBracket);

    auto begin = pos;
    auto paren_count = 0 ;
    String kql_statement;

    if (s_lparen.ignore(pos, expected))
    {
        ++paren_count;
        while (!pos->isEnd())
        {
            if (pos->type == TokenType::ClosingRoundBracket)
                --paren_count;
            if (pos->type == TokenType::OpeningRoundBracket)
                ++paren_count;

            if (paren_count == 0)
                break;

            kql_statement = kql_statement + " " + String(pos->begin,pos->end);
            ++pos;
        }

        Tokens token_kql(kql_statement.c_str(), kql_statement.c_str() + kql_statement.size());
        IParser::Pos pos_kql(token_kql, pos.max_depth);

        if (kql_p.parse(pos_kql, select, expected))
        {
            node = select;
            ++pos;
            return true;
        }
    }
    pos =  begin;
    return false;
};

}
