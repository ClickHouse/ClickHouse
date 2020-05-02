#include <Parsers/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/parseUserName.h>
#include <assert.h>


namespace DB
{
bool ParserShowCreateAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SHOW CREATE"}.ignore(pos, expected))
        return false;

    using Kind = ASTShowCreateAccessEntityQuery::Kind;
    Kind kind;
    if (ParserKeyword{"USER"}.ignore(pos, expected))
        kind = Kind::USER;
    else if (ParserKeyword{"QUOTA"}.ignore(pos, expected))
        kind = Kind::QUOTA;
    else if (ParserKeyword{"POLICY"}.ignore(pos, expected) || ParserKeyword{"ROW POLICY"}.ignore(pos, expected))
        kind = Kind::ROW_POLICY;
    else if (ParserKeyword{"ROLE"}.ignore(pos, expected))
        kind = Kind::ROLE;
    else if (ParserKeyword{"SETTINGS PROFILE"}.ignore(pos, expected) || ParserKeyword{"PROFILE"}.ignore(pos, expected))
        kind = Kind::SETTINGS_PROFILE;
    else
        return false;

    String name;
    bool current_quota = false;
    bool current_user = false;
    RowPolicy::NameParts row_policy_name_parts;

    if (kind == Kind::USER)
    {
        if (!parseUserNameOrCurrentUserTag(pos, expected, name, current_user))
            current_user = true;
    }
    else if (kind == Kind::ROLE)
    {
        if (!parseRoleName(pos, expected, name))
            return false;
    }
    else if (kind == Kind::ROW_POLICY)
    {
        String & database = row_policy_name_parts.database;
        String & table_name = row_policy_name_parts.table_name;
        String & short_name = row_policy_name_parts.short_name;
        if (!parseIdentifierOrStringLiteral(pos, expected, short_name) || !ParserKeyword{"ON"}.ignore(pos, expected)
            || !parseDatabaseAndTableName(pos, expected, database, table_name))
            return false;
    }
    else if (kind == Kind::QUOTA)
    {
        if (ParserKeyword{"CURRENT"}.ignore(pos, expected))
        {
            /// SHOW CREATE QUOTA CURRENT
            current_quota = true;
        }
        else if (parseIdentifierOrStringLiteral(pos, expected, name))
        {
            /// SHOW CREATE QUOTA name
        }
        else
        {
            /// SHOW CREATE QUOTA
            current_quota = true;
        }
    }
    else if (kind == Kind::SETTINGS_PROFILE)
    {
        if (!parseIdentifierOrStringLiteral(pos, expected, name))
            return false;
    }

    auto query = std::make_shared<ASTShowCreateAccessEntityQuery>(kind);
    node = query;

    query->name = std::move(name);
    query->current_quota = current_quota;
    query->current_user = current_user;
    query->row_policy_name_parts = std::move(row_policy_name_parts);

    return true;
}
}
