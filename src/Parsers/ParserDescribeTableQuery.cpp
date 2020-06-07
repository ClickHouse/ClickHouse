#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserDescribeTableQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    ParserKeyword s_describe("DESCRIBE");
    ParserKeyword s_desc("DESC");
    ParserKeyword s_table("TABLE");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;

    ASTPtr database;
    ASTPtr table;

    if (!s_describe.ignore(pos, expected, ranges) && !s_desc.ignore(pos, expected, ranges))
        return false;

    auto query = std::make_shared<ASTDescribeQuery>();

    s_table.ignore(pos, expected, ranges);

    ASTPtr table_expression;
    if (!ParserTableExpression().parse(pos, table_expression, expected, ranges))
        return false;

    query->table_expression = table_expression;

    node = query;

    return true;
}


}
