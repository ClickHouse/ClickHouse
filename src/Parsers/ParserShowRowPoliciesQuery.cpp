#include <Parsers/ParserShowRowPoliciesQuery.h>
#include <Parsers/ASTShowRowPoliciesQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{
namespace
{
    bool parseONDatabaseAndTableName(IParserBase::Pos & pos, Expected & expected, String & database, String & table_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            database.clear();
            table_name.clear();
            return ParserKeyword{"ON"}.ignore(pos, expected) && parseDatabaseAndTableName(pos, expected, database, table_name);
        });
    }
}


bool ParserShowRowPoliciesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SHOW POLICIES"}.ignore(pos, expected) && !ParserKeyword{"SHOW ROW POLICIES"}.ignore(pos, expected))
        return false;

    bool current = ParserKeyword{"CURRENT"}.ignore(pos, expected);

    String database, table_name;
    parseONDatabaseAndTableName(pos, expected, database, table_name);

    auto query = std::make_shared<ASTShowRowPoliciesQuery>();
    query->current = current;
    query->database = std::move(database);
    query->table_name = std::move(table_name);
    node = query;
    return true;
}
}
