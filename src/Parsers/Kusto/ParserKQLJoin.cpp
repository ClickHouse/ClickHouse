#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLJoin.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/Utilities.h>
#include <Parsers/ParserTablesInSelectQuery.h>

#include <Poco/String.h>

#include <fmt/format.h>

namespace DB
{

bool ParserKQLJoin::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// join kind=<kind> (<right_expression>) on <conditions>
    /// Supported kinds: inner, leftouter, rightouter, fullouter, leftanti, leftantisemi,
    ///                  leftsemi, rightanti, rightantisemi, rightsemi

    if (!isValidKQLPos(pos))
        return false;

    /// Parse optional kind=<type>
    /// KQL keywords are case-insensitive, so normalize before comparison.
    String join_kind = "innerunique"; /// default
    const String token = Poco::toLower(String(pos->begin, pos->end));
    if (token == "kind")
    {
        ++pos;
        if (!isValidKQLPos(pos) || String(pos->begin, pos->end) != "=")
            return false;
        ++pos;
        if (!isValidKQLPos(pos))
            return false;
        join_kind = Poco::toLower(String(pos->begin, pos->end));
        ++pos;
    }

    /// Map KQL join kind to ClickHouse join type.
    /// Reject unsupported `kind=...` values rather than silently treating them as `INNER`,
    /// which would mask typos and unsupported kinds with semantically different results.
    String ch_join_type;
    if (join_kind == "inner" || join_kind == "innerunique")
        ch_join_type = "INNER";
    else if (join_kind == "leftouter")
        ch_join_type = "LEFT";
    else if (join_kind == "rightouter")
        ch_join_type = "RIGHT";
    else if (join_kind == "fullouter")
        ch_join_type = "FULL";
    else if (join_kind == "leftanti" || join_kind == "leftantisemi")
        ch_join_type = "LEFT ANTI";
    else if (join_kind == "leftsemi")
        ch_join_type = "LEFT SEMI";
    else if (join_kind == "rightanti" || join_kind == "rightantisemi")
        ch_join_type = "RIGHT ANTI";
    else if (join_kind == "rightsemi")
        ch_join_type = "RIGHT SEMI";
    else
        return false;

    /// Parse the right-side subquery in parentheses
    if (!isValidKQLPos(pos) || pos->type != TokenType::OpeningRoundBracket)
        return false;

    /// Find matching closing bracket
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

    /// Guard: closing bracket missing
    if (!isValidKQLPos(pos) || pos->type != TokenType::ClosingRoundBracket)
        return false;
    /// Guard: empty parenthesized subquery (`join (...) ()`).
    /// We must check before decrementing `end_pos`, otherwise `--end_pos` would step
    /// before `content_start` and `String(content_start->begin, end_pos->end)` would
    /// be built from an inverted iterator range.
    if (pos == content_start)
        return false;
    /// pos is at the closing bracket. Get text from content_start to the token before pos.
    auto end_pos = pos;
    --end_pos;
    String right_query(content_start->begin, end_pos->end);
    ++pos; /// skip closing bracket

    /// Parse "on" keyword and conditions
    if (!isValidKQLPos(pos))
        return false;
    const String on_token = Poco::toLower(String(pos->begin, pos->end));
    if (on_token != "on")
        return false;
    ++pos;

    /// Collect on-conditions
    String on_conditions;
    while (isValidKQLPos(pos) && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (!on_conditions.empty())
            on_conditions += " ";
        on_conditions += String(pos->begin, pos->end);
        ++pos;
    }

    /// Build a SQL query from the join
    /// The left side is already in node as the table source
    /// We need to add the join clause to the tables expression

    /// Build the join as: SELECT * FROM (<left>) <JOIN_TYPE> JOIN (<right>) ON <conditions>
    /// For simplicity, we'll construct this as a string and reparse

    /// Parse the right subquery as KQL
    ASTPtr right_select_node;
    {
        Tokens right_tokens(right_query.data(), right_query.data() + right_query.size(), 0, true);
        IParser::Pos right_pos(right_tokens, pos);
        if (!ParserKQLWithUnionQuery().parse(right_pos, right_select_node, expected))
            return false;
    }

    /// Build join conditions - handle simple column name joins
    /// In KQL "on col" means "on left.col = right.col"
    /// In KQL "on col1, col2" means "on left.col1 = right.col1 AND left.col2 = right.col2"
    /// We add suffix "1" for the right table columns (ClickHouse join convention)
    String sql_on;
    if (on_conditions.find("==") == String::npos && on_conditions.find('=') == String::npos)
    {
        /// Simple column name(s) - KQL convention: on col means left.col = right.col
        /// Handle comma-separated keys: on a, b -> a = a1 AND b = b1
        /// ClickHouse renames duplicate columns with suffix "1" in joins
        std::vector<String> keys;
        {
            String current;
            for (char c : on_conditions)
            {
                if (c == ',')
                {
                    /// Trim whitespace from key
                    auto start = current.find_first_not_of(' ');
                    auto end = current.find_last_not_of(' ');
                    if (start != String::npos)
                        keys.push_back(current.substr(start, end - start + 1));
                    current.clear();
                }
                else
                    current += c;
            }
            auto start = current.find_first_not_of(' ');
            auto end = current.find_last_not_of(' ');
            if (start != String::npos)
                keys.push_back(current.substr(start, end - start + 1));
        }
        for (size_t i = 0; i < keys.size(); ++i)
        {
            if (i > 0)
                sql_on += " AND ";
            sql_on += fmt::format("{0} = {0}1", keys[i]);
        }
    }
    else
    {
        sql_on = on_conditions;
    }

    /// Now build the full join using the tables expression
    auto * select = node->as<ASTSelectQuery>();

    /// Create the join table expression
    ASTPtr right_subquery = make_intrusive<ASTSubquery>(std::move(right_select_node));

    ASTPtr right_table_expr = make_intrusive<ASTTableExpression>();
    right_table_expr->as<ASTTableExpression>()->subquery = right_subquery;
    right_table_expr->children.emplace_back(right_subquery);

    /// Create join element
    auto join_element = make_intrusive<ASTTablesInSelectQueryElement>();
    auto table_join = make_intrusive<ASTTableJoin>();

    if (ch_join_type == "INNER")
    {
        table_join->kind = JoinKind::Inner;
        table_join->strictness = JoinStrictness::All;
    }
    else if (ch_join_type == "LEFT")
    {
        table_join->kind = JoinKind::Left;
        table_join->strictness = JoinStrictness::All;
    }
    else if (ch_join_type == "RIGHT")
    {
        table_join->kind = JoinKind::Right;
        table_join->strictness = JoinStrictness::All;
    }
    else if (ch_join_type == "FULL")
    {
        table_join->kind = JoinKind::Full;
        table_join->strictness = JoinStrictness::All;
    }
    else if (ch_join_type == "LEFT ANTI")
    {
        table_join->kind = JoinKind::Left;
        table_join->strictness = JoinStrictness::Anti;
    }
    else if (ch_join_type == "LEFT SEMI")
    {
        table_join->kind = JoinKind::Left;
        table_join->strictness = JoinStrictness::Semi;
    }
    else if (ch_join_type == "RIGHT ANTI")
    {
        table_join->kind = JoinKind::Right;
        table_join->strictness = JoinStrictness::Anti;
    }
    else if (ch_join_type == "RIGHT SEMI")
    {
        table_join->kind = JoinKind::Right;
        table_join->strictness = JoinStrictness::Semi;
    }
    else
    {
        table_join->kind = JoinKind::Inner;
        table_join->strictness = JoinStrictness::All;
    }

    /// Parse on expression
    String on_expr_str = sql_on;
    Tokens on_tokens(on_expr_str.data(), on_expr_str.data() + on_expr_str.size(), 0, true);
    IParser::Pos on_pos(on_tokens, pos);
    ASTPtr on_expr;
    if (!ParserExpressionWithOptionalAlias(false).parse(on_pos, on_expr, expected))
        return false;

    table_join->on_expression = on_expr;
    table_join->children.push_back(on_expr);

    join_element->table_join = table_join;
    join_element->table_expression = right_table_expr;
    join_element->children.push_back(table_join);
    join_element->children.push_back(right_table_expr);

    /// Add join to existing tables
    if (select->tables())
    {
        select->tables()->children.push_back(join_element);
    }

    return true;
}

}
