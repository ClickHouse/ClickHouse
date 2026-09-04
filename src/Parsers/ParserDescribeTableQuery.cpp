#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{

bool ParserDescribeTableQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_describe(Keyword::DESCRIBE);
    ParserKeyword s_desc(Keyword::DESC);
    ParserKeyword s_table(Keyword::TABLE);
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserSetQuery parser_settings(true);

    ASTPtr select;

    if (!s_describe.ignore(pos, expected) && !s_desc.ignore(pos, expected))
        return false;

    auto query = make_intrusive<ASTDescribeQuery>();

    s_table.ignore(pos, expected);

    /// Try to parse SELECT query without parentheses (e.g., DESCRIBE SELECT 1)
    if (ParserSelectWithUnionQuery().parse(pos, select, expected))
    {
        auto table_expr = make_intrusive<ASTTableExpression>();
        /// Wrap SELECT in ASTSubquery, as expected by the rest of the codebase
        auto subquery = make_intrusive<ASTSubquery>(std::move(select));
        table_expr->subquery = subquery;
        table_expr->children.push_back(table_expr->subquery);
        query->table_expression = table_expr;
    }
    else if (!ParserTableExpression().parse(pos, query->table_expression, expected))
        return false;

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
