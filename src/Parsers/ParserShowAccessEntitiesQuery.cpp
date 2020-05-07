#include <Parsers/ParserShowAccessEntitiesQuery.h>
#include <Parsers/ASTShowAccessEntitiesQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{
namespace
{
    using EntityType = IAccessEntity::Type;

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


bool ParserShowAccessEntitiesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SHOW POLICIES"}.ignore(pos, expected) && !ParserKeyword{"SHOW ROW POLICIES"}.ignore(pos, expected))
        return false;

    String database, table_name;
    parseONDatabaseAndTableName(pos, expected, database, table_name);

    auto query = std::make_shared<ASTShowAccessEntitiesQuery>();
    node = query;

    query->type = EntityType::ROW_POLICY;
    query->database = std::move(database);
    query->table_name = std::move(table_name);

    return true;
}
}
