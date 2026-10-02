#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/ParserKQLUnion.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserSelectWithUnionQuery.h>

#include <fmt/format.h>

namespace DB
{

bool ParserKQLUnion::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// union (<right_expression>) [| union (<right_expression2>) ...]
    /// In KQL, union combines the results of two or more tables

    if (!isValidKQLPos(pos) || pos->type != TokenType::OpeningRoundBracket)
        return false;

    /// Find matching closing bracket for the right-side subquery
    int bracket_depth = 1;
    ++pos;
    auto content_start = pos;
    while (isValidKQLPos(pos) && bracket_depth > 0)
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++bracket_depth;
        if (pos->type == TokenType::ClosingRoundBracket)
            --bracket_depth;
        if (bracket_depth > 0)
            ++pos;
    }

    /// Guard: if closing bracket was not found, bail out
    if (!isValidKQLPos(pos) || pos->type != TokenType::ClosingRoundBracket)
        return false;
    /// Guard: empty parenthesized subquery, e.g. union ()
    if (content_start->begin >= pos->begin)
        return false;
    String right_query(content_start->begin, pos->begin);
    ++pos; /// skip closing bracket

    /// Parse the right subquery as KQL
    ASTPtr right_select_node;
    Tokens right_tokens(right_query.data(), right_query.data() + right_query.size(), 0, true);
    IParser::Pos right_pos(right_tokens, pos);
    if (!ParserKQLWithUnionQuery().parse(right_pos, right_select_node, expected))
        return false;

    /// Get the current select from node and the right select
    /// Build a UNION ALL by wrapping both in ASTSelectWithUnionQuery
    auto * current_select = node->as<ASTSelectQuery>();
    if (!current_select)
        return false;

    /// We need to set the select expression on the node
    /// Use a subquery approach: SELECT * FROM (left UNION ALL right)

    /// Get the left query's SQL representation
    String left_sql = node->formatWithSecretsOneLine();
    String right_sql = right_select_node->formatWithSecretsOneLine();

    /// Build UNION ALL query
    String union_query = fmt::format("SELECT * FROM ({} UNION ALL {})", left_sql, right_sql);

    /// Parse the union query as standard SQL and use its tables expression
    Tokens union_tokens(union_query.data(), union_query.data() + union_query.size(), 0, true);
    IParser::Pos union_pos(union_tokens, pos);

    ASTPtr union_select;
    ParserSelectWithUnionQuery union_parser;
    if (!union_parser.parse(union_pos, union_select, expected))
        return false;

    /// Replace node's content with the union result
    /// Extract the subquery and set it as the source
    ASTPtr subquery_node = make_intrusive<ASTSubquery>(std::move(union_select));

    ASTPtr table_expr = make_intrusive<ASTTableExpression>();
    table_expr->as<ASTTableExpression>()->subquery = subquery_node;
    table_expr->children.emplace_back(subquery_node);

    auto table_element = make_intrusive<ASTTablesInSelectQueryElement>();
    table_element->as<ASTTablesInSelectQueryElement>()->table_expression = table_expr;
    table_element->children.emplace_back(table_expr);

    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    tables->children.emplace_back(table_element);

    /// Reset the select query.
    /// We rebuild the outer query as `SELECT * FROM (left UNION ALL right)`, where
    /// the original left-side clauses are already serialized into `left_sql`.
    /// Therefore any clauses still attached to `current_select` from earlier pipe
    /// operators (`WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, ...) would be
    /// applied a second time *after* the union, changing semantics. Clear them so
    /// only the new `SELECT *` / `FROM (...)` shape remains.
    current_select->setExpression(ASTSelectQuery::Expression::SELECT, nullptr);
    current_select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
    current_select->setExpression(ASTSelectQuery::Expression::PREWHERE, nullptr);
    current_select->setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
    current_select->setExpression(ASTSelectQuery::Expression::GROUP_BY, nullptr);
    current_select->setExpression(ASTSelectQuery::Expression::HAVING, nullptr);
    current_select->setExpression(ASTSelectQuery::Expression::WINDOW, nullptr);
    current_select->setExpression(ASTSelectQuery::Expression::ORDER_BY, nullptr);
    current_select->setExpression(ASTSelectQuery::Expression::LIMIT_BY_OFFSET, nullptr);
    current_select->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, nullptr);
    current_select->setExpression(ASTSelectQuery::Expression::LIMIT_BY, nullptr);
    current_select->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, nullptr);
    current_select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, nullptr);

    /// Set project to *
    String star = "*";
    Tokens star_tokens(star.data(), star.data() + star.size(), 0, true);
    IParser::Pos star_pos(star_tokens, pos);
    ASTPtr select_expr;
    if (!ParserNotEmptyExpressionList(true).parse(star_pos, select_expr, expected))
        return false;
    current_select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr));

    return true;
}

}
