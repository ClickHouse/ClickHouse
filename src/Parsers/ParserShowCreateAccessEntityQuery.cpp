#include <Parsers/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/parseUserName.h>
#include <ext/range.h>
#include <assert.h>


namespace DB
{
using EntityType = IAccessEntity::Type;
using EntityTypeInfo = IAccessEntity::TypeInfo;


bool ParserShowCreateAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SHOW CREATE"}.ignore(pos, expected))
        return false;

    std::optional<EntityType> type;
    for (auto type_i : ext::range(EntityType::MAX))
    {
        const auto & type_info = EntityTypeInfo::get(type_i);
        if (ParserKeyword{type_info.name.c_str()}.ignore(pos, expected)
            || (!type_info.alias.empty() && ParserKeyword{type_info.alias.c_str()}.ignore(pos, expected)))
        {
            type = type_i;
        }
    }
    if (!type)
        return false;

    String name;
    bool current_quota = false;
    bool current_user = false;
    RowPolicy::NameParts row_policy_name_parts;

    if (type == EntityType::USER)
    {
        if (!parseUserNameOrCurrentUserTag(pos, expected, name, current_user))
            current_user = true;
    }
    else if (type == EntityType::ROLE)
    {
        if (!parseRoleName(pos, expected, name))
            return false;
    }
    else if (type == EntityType::ROW_POLICY)
    {
        String & database = row_policy_name_parts.database;
        String & table_name = row_policy_name_parts.table_name;
        String & short_name = row_policy_name_parts.short_name;
        if (!parseIdentifierOrStringLiteral(pos, expected, short_name) || !ParserKeyword{"ON"}.ignore(pos, expected)
            || !parseDatabaseAndTableName(pos, expected, database, table_name))
            return false;
    }
    else if (type == EntityType::QUOTA)
    {
        if (!parseIdentifierOrStringLiteral(pos, expected, name))
        {
            /// SHOW CREATE QUOTA
            current_quota = true;
        }
    }
    else if (type == EntityType::SETTINGS_PROFILE)
    {
        if (!parseIdentifierOrStringLiteral(pos, expected, name))
            return false;
    }

    auto query = std::make_shared<ASTShowCreateAccessEntityQuery>();
    node = query;

    query->type = *type;
    query->name = std::move(name);
    query->current_quota = current_quota;
    query->current_user = current_user;
    query->row_policy_name_parts = std::move(row_policy_name_parts);

    return true;
}
}
