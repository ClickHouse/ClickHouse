#include <Parsers/ASTQueryWithTableAndOutput.h>

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
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;
    ParserSetQuery parser_settings(true);

    ASTPtr database;
    ASTPtr table;

    if (!s_describe.ignore(pos, expected) && !s_desc.ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTDescribeQuery>();

    s_table.ignore(pos, expected);

    ASTPtr table_expression;
    if (!ParserTableExpression().parse(pos, table_expression, expected))
        return false;

    /// For compatibility with SELECTs, where SETTINGS can be in front of FORMAT
    ASTPtr settings;
    if (s_settings.ignore(pos, expected))
    {
        ASTPtr settings_node;
        if (!parser_settings.parse(pos, settings_node, expected))
            return false;
        query->setSettingsAST(settings_node);
    }

    query->set(query->table_expression, table_expression);
    node = std::move(query);

    return true;
}


}
