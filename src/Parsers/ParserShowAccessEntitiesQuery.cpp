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
    if (!ParserKeyword{"SHOW"}.ignore(pos, expected))
        return false;

    std::optional<EntityType> type;
    bool current_quota = false;
    bool current_roles = false;
    bool enabled_roles = false;

    if (ParserKeyword{"USERS"}.ignore(pos, expected))
    {
        type = EntityType::USER;
    }
    else if (ParserKeyword{"ROLES"}.ignore(pos, expected))
    {
        type = EntityType::ROLE;
    }
    else if (ParserKeyword{"CURRENT ROLES"}.ignore(pos, expected))
    {
        type = EntityType::ROLE;
        current_roles = true;
    }
    else if (ParserKeyword{"ENABLED ROLES"}.ignore(pos, expected))
    {
        type = EntityType::ROLE;
        enabled_roles = true;
    }
    else if (ParserKeyword{"POLICIES"}.ignore(pos, expected) || ParserKeyword{"ROW POLICIES"}.ignore(pos, expected))
    {
        type = EntityType::ROW_POLICY;
    }
    else if (ParserKeyword{"QUOTAS"}.ignore(pos, expected))
    {
        type = EntityType::QUOTA;
    }
    else if (ParserKeyword{"QUOTA"}.ignore(pos, expected) || ParserKeyword{"CURRENT QUOTA"}.ignore(pos, expected))
    {
        type = EntityType::QUOTA;
        current_quota = true;
    }
    else if (ParserKeyword{"PROFILES"}.ignore(pos, expected) || ParserKeyword{"SETTINGS PROFILES"}.ignore(pos, expected))
    {
        type = EntityType::SETTINGS_PROFILE;
    }
    else
        return false;

    String database, table_name;
    if (type == EntityType::ROW_POLICY)
        parseONDatabaseAndTableName(pos, expected, database, table_name);

    auto query = std::make_shared<ASTShowAccessEntitiesQuery>();
    node = query;

    query->type = *type;
    query->current_quota = current_quota;
    query->current_roles = current_roles;
    query->enabled_roles = enabled_roles;
    query->database = std::move(database);
    query->table_name = std::move(table_name);

    return true;
}
}
