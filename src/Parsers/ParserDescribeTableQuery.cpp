#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
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

    ASTPtr database;
    ASTPtr table;

    if (!s_describe.ignore(pos, expected) && !s_desc.ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTDescribeQuery>();

    s_table.ignore(pos, expected);

    if (!ParserTableExpression().parse(pos, query->table_expression, expected))
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
