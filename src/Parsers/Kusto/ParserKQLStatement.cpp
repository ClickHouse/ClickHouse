#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

bool ParserKQLStatement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKQLWithOutput query_with_output_p(end, allow_settings_after_format_in_insert);
    ParserSetQuery set_p;

    bool res = query_with_output_p.parse(pos, node, expected) || set_p.parse(pos, node, expected);

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

bool ParserKQLTableFunction::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// TODO: This code is idiotic, see https://github.com/ClickHouse/ClickHouse/issues/61742

    ParserToken lparen(TokenType::OpeningRoundBracket);

    ASTPtr string_literal;
    ParserStringLiteral parser_string_literal;

    if (!lparen.ignore(pos, expected))
        return false;

    size_t paren_count = 0;
    String kql_statement;
    if (parser_string_literal.parse(pos, string_literal, expected))
    {
        kql_statement = typeid_cast<const ASTLiteral &>(*string_literal).value.safeGet<String>();
    }
    else
    {
        ++paren_count;
        auto pos_start = pos;
        while (isValidKQLPos(pos))
        {
            if (pos->type == TokenType::ClosingRoundBracket)
                --paren_count;
            if (pos->type == TokenType::OpeningRoundBracket)
                ++paren_count;

            if (paren_count == 0)
                break;
            ++pos;
        }
        if (!isValidKQLPos(pos))
        {
            return false;
        }
        --pos;
        kql_statement = String(pos_start->begin, pos->end);
        ++pos;
    }

    Tokens tokens_kql(kql_statement.data(), kql_statement.data() + kql_statement.size(), 0, true);
    IParser::Pos pos_kql(tokens_kql, pos.max_depth, pos.max_backtracks);

    Expected kql_expected;
    kql_expected.enable_highlighting = false;
    if (!ParserKQLWithUnionQuery().parse(pos_kql, node, kql_expected))
        return false;

    ++pos;
    return true;
}

}
