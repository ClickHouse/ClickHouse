#include <Parsers/Access/ParserShowAccessEntitiesQuery.h>
#include <Parsers/Access/ASTShowAccessEntitiesQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <base/range.h>


namespace DB
{
namespace
{
    bool parseEntityType(IParserBase::Pos & pos, Expected & expected, AccessEntityType & type)
    {
        for (auto i : collections::range(AccessEntityType::MAX))
        {
            const auto & type_info = AccessEntityTypeInfo::get(i);
            if (ParserKeyword::createDeprecated(type_info.plural_name).ignore(pos, expected)
                || (!type_info.plural_alias.empty() && ParserKeyword::createDeprecated(type_info.plural_alias).ignore(pos, expected)))
            {
                type = i;
                return true;
            }
        }
        return false;
    }

    bool parseOnDBAndTableName(IParserBase::Pos & pos, Expected & expected, String & database, String & table, bool & wildcard, bool & default_database)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::ON}.ignore(pos, expected)
                && parseDatabaseAndTableNameOrAsterisks(pos, expected, database, table, wildcard, default_database);
        });
    }
}


bool ParserShowAccessEntitiesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::SHOW}.ignore(pos, expected))
        return false;

    AccessEntityType type;
    bool all = false;
    bool current_quota = false;
    bool current_roles = false;
    bool enabled_roles = false;

    if (parseEntityType(pos, expected, type))
    {
        all = true;
    }
    else if (ParserKeyword{Keyword::CURRENT_ROLES}.ignore(pos, expected))
    {
        type = AccessEntityType::ROLE;
        current_roles = true;
    }
    else if (ParserKeyword{Keyword::ENABLED_ROLES}.ignore(pos, expected))
    {
        type = AccessEntityType::ROLE;
        enabled_roles = true;
    }
    else if (ParserKeyword{Keyword::CURRENT_QUOTA}.ignore(pos, expected) || ParserKeyword{Keyword::QUOTA}.ignore(pos, expected))
    {
        type = AccessEntityType::QUOTA;
        current_quota = true;
    }
    else
        return false;

    String short_name;
    std::optional<std::pair<String, String>> database_and_table_name;
    if (type == AccessEntityType::ROW_POLICY)
    {
        String database, table_name;
        bool wildcard = false;
        bool default_database = false;
        if (parseOnDBAndTableName(pos, expected, database, table_name, wildcard, default_database))
        {
            if (database.empty() && !default_database)
                all = true;
            else
                database_and_table_name.emplace(database, table_name);
        }
        else if (parseIdentifierOrStringLiteral(pos, expected, short_name))
        {
        }
        else
            all = true;
    }

    auto query = std::make_shared<ASTShowAccessEntitiesQuery>();
    node = query;

    query->type = type;
    query->all = all;
    query->current_quota = current_quota;
    query->current_roles = current_roles;
    query->enabled_roles = enabled_roles;
    query->short_name = std::move(short_name);
    query->database_and_table_name = std::move(database_and_table_name);

    return true;
}
}
