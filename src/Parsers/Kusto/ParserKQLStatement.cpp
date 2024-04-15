#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{

bool ParserKQLStatement::parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected)
{
    ParserKQLWithOutput query_with_output_p(end, allow_settings_after_format_in_insert);

    bool res = query_with_output_p.parse(pos, node, expected);
    if (!res)
    {
        String query;
        ParserSetQuery set_p;
        Expected sql_expected;
        while (pos.isValid() && pos->type != KQLTokenType::Semicolon)
        {
            query += " " + String(pos->begin, pos->end);
            ++pos;
        }
        Tokens sql_tokens(query.data(), query.data() + query.size());
        IParser::Pos sql_pos(sql_tokens, pos.max_depth, pos.max_backtracks);
        res = set_p.parse(sql_pos, node, sql_expected);
    }

    return res;
}

bool ParserKQLWithOutput::parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected)
{
    ParserKQLWithUnionQuery kql_p;

    ASTPtr query;
    bool parsed = kql_p.parse(pos, query, expected);

    if (!parsed)
        return false;

    node = std::move(query);
    return true;
}

bool ParserKQLWithUnionQuery::parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected)
{
    // will support union next phase
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

bool ParserKQLTableFunction::parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected)
{
    ParserKQLToken lparen(KQLTokenType::OpeningRoundBracket);

    if (!lparen.ignore(pos, expected))
        return false;

    size_t paren_count = 0;
    String kql_statement;

    ++paren_count;
    auto pos_start = pos;
    while (isValidKQLPos(pos))
    {
        if (pos->type == KQLTokenType::ClosingRoundBracket)
            --paren_count;
        if (pos->type == KQLTokenType::OpeningRoundBracket)
            ++paren_count;

        if (paren_count == 0)
            break;
        ++pos;
    }
    --pos;
    kql_statement = String(pos_start->begin, pos->end);
    ++pos;

    KQLTokens token_kql(kql_statement.data(), kql_statement.data() + kql_statement.size(), 0, true);
    IKQLParser::KQLPos pos_kql(token_kql, pos.max_depth, pos.max_backtracks);

    if (!ParserKQLWithUnionQuery().parse(pos_kql, node, expected))
        return false;
    ++pos;
    return true;
}

}
