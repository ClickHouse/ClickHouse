#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{

namespace
{

/// The input begins with `(`. Decide whether the leading parenthesis encloses only the first
/// element of a top-level set operation, e.g. `(SELECT 1) UNION ALL (SELECT 2)`, as opposed to a
/// single parenthesized subquery used as a table expression, e.g. `(SELECT 1 UNION ALL SELECT 2)`
/// or `(SELECT 1) AS source`. We skip past the parenthesis matching the leading `(` and check
/// whether it is immediately followed by a set-operation keyword.
bool isTopLevelSetOperation(IParser::Pos pos)
{
    int depth = 0;
    do
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++depth;
        else if (pos->type == TokenType::ClosingRoundBracket)
            --depth;
        ++pos;
    } while (depth > 0 && pos.isValid());

    Expected expected;
    return ParserKeyword(Keyword::UNION).ignore(pos, expected)
        || ParserKeyword(Keyword::EXCEPT).ignore(pos, expected)
        || ParserKeyword(Keyword::INTERSECT).ignore(pos, expected);
}

}

bool ParserDescribeTableQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_describe(Keyword::DESCRIBE);
    ParserKeyword s_desc(Keyword::DESC);
    ParserKeyword s_temporary(Keyword::TEMPORARY);
    ParserKeyword s_table(Keyword::TABLE);
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserSetQuery parser_settings(true);

    ASTPtr select;

    if (!s_describe.ignore(pos, expected) && !s_desc.ignore(pos, expected))
        return false;

    auto query = make_intrusive<ASTDescribeQuery>();

    /// TEMPORARY is only recognized in the explicit "DESCRIBE TEMPORARY TABLE ..." form.
    /// This avoids breaking "DESCRIBE temporary" where `temporary` is an unquoted table name.
    bool temporary = false;
    {
        auto saved_pos = pos;
        if (s_temporary.ignore(pos, expected))
        {
            if (s_table.ignore(pos, expected))
            {
                temporary = true;
            }
            else
            {
                /// Not followed by TABLE — revert so `temporary` is parsed as a table name.
                pos = saved_pos;
                s_table.ignore(pos, expected);
            }
        }
        else
        {
            s_table.ignore(pos, expected);
        }
    }

    /// A SELECT can be the source of DESCRIBE in several forms:
    ///   - a bare SELECT:                 DESCRIBE SELECT 1
    ///   - a set operation:               DESCRIBE (SELECT 1) UNION ALL (SELECT 2)
    /// while a single parenthesized SELECT is a table expression that may carry an alias:
    ///   - a subquery with an alias:      DESCRIBE (SELECT 1) AS source
    ///   - a subquery containing a set operation with an alias:
    ///                                    DESCRIBE (SELECT 1 UNION ALL SELECT 2) AS source
    /// The latter must go through ParserTableExpression so the trailing alias is accepted.
    /// To tell them apart we speculatively parse a SELECT. We keep it when it is a bare SELECT or
    /// a genuine top-level set operation; a lone parenthesized SELECT (even one whose body is a set
    /// operation) is rolled back and handled as a table expression below. Note that
    /// ParserSelectWithUnionQuery lifts up a single parenthesized inner union, so the size of
    /// list_of_selects cannot distinguish `(SELECT 1) UNION ALL (SELECT 2)` from
    /// `(SELECT 1 UNION ALL SELECT 2)`; we instead inspect what follows the leading parenthesis.
    bool parsed_as_select = false;
    {
        auto saved_pos = pos;
        ASTPtr parsed_select;
        if (ParserSelectWithUnionQuery().parse(pos, parsed_select, expected))
        {
            const bool keep_select = saved_pos->type != TokenType::OpeningRoundBracket
                || isTopLevelSetOperation(saved_pos);

            if (keep_select)
            {
                select = std::move(parsed_select);
                parsed_as_select = true;
            }
            else
            {
                /// A single parenthesized SELECT: roll back and let ParserTableExpression pick up the alias.
                pos = saved_pos;
            }
        }
    }

    if (parsed_as_select)
    {
        /// TEMPORARY is only valid with a table name, not with a subquery or SELECT
        if (temporary)
            return false;

        auto table_expr = make_intrusive<ASTTableExpression>();
        /// Wrap SELECT in ASTSubquery, as expected by the rest of the codebase
        auto subquery = make_intrusive<ASTSubquery>(std::move(select));
        table_expr->subquery = subquery;
        table_expr->children.push_back(table_expr->subquery);
        query->table_expression = table_expr;
    }
    else if (!ParserTableExpression().parse(pos, query->table_expression, expected))
    {
        return false;
    }
    else if (temporary)
    {
        /// TEMPORARY is only valid with a table name, not with a table function or subquery
        auto * table_expr = query->table_expression->as<ASTTableExpression>();
        if (!table_expr || !table_expr->database_and_table_name)
            return false;
        query->temporary = true;
    }

    /// For compatibility with SELECTs, where SETTINGS can be in front of FORMAT
    ASTPtr settings;
    if (s_settings.ignore(pos, expected))
    {
        if (!parser_settings.parse(pos, query->settings_ast, expected))
            return false;
    }

    query->children.push_back(query->table_expression);

    if (query->settings_ast)
        query->children.push_back(query->settings_ast);

    node = query;

    return true;
}

}
