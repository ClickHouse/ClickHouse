#include <Parsers/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ParserRowPolicyName.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Parsers/parseUserName.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <ext/range.h>
#include <assert.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace
{
    using EntityType = IAccessEntity::Type;
    using EntityTypeInfo = IAccessEntity::TypeInfo;

    bool parseEntityType(IParserBase::Pos & pos, Expected & expected, EntityType & type, bool & plural)
    {
        for (auto i : ext::range(EntityType::MAX))
        {
            const auto & type_info = EntityTypeInfo::get(i);
            if (ParserKeyword{type_info.name.c_str()}.ignore(pos, expected)
                || (!type_info.alias.empty() && ParserKeyword{type_info.alias.c_str()}.ignore(pos, expected)))
            {
                type = i;
                plural = false;
                return true;
            }
        }

        for (auto i : ext::range(EntityType::MAX))
        {
            const auto & type_info = EntityTypeInfo::get(i);
            if (ParserKeyword{type_info.plural_name.c_str()}.ignore(pos, expected)
                || (!type_info.plural_alias.empty() && ParserKeyword{type_info.plural_alias.c_str()}.ignore(pos, expected)))
            {
                type = i;
                plural = true;
                return true;
            }
        }

        return false;
    }

    bool parseOnDBAndTableName(IParserBase::Pos & pos, Expected & expected, String & database, bool & any_database, String & table, bool & any_table)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{"ON"}.ignore(pos, expected)
                && parseDatabaseAndTableNameOrAsterisks(pos, expected, database, any_database, table, any_table);
        });
    }
}


bool ParserShowCreateAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SHOW CREATE"}.ignore(pos, expected))
        return false;

    EntityType type;
    bool plural;
    if (!parseEntityType(pos, expected, type, plural))
        return false;

    Strings names;
    std::shared_ptr<ASTRowPolicyNames> row_policy_names;
    bool all = false;
    bool current_quota = false;
    bool current_user = false;
    String short_name;
    std::optional<std::pair<String, String>> database_and_table_name;

    switch (type)
    {
        case EntityType::USER:
        {
            if (parseCurrentUserTag(pos, expected))
                current_user = true;
            else if (parseUserNames(pos, expected, names))
            {
            }
            else if (plural)
                all = true;
            else
                current_user = true;
            break;
        }
        case EntityType::ROLE:
        {
            if (parseRoleNames(pos, expected, names))
            {
            }
            else if (plural)
                all = true;
            else
                return false;
            break;
        }
        case EntityType::ROW_POLICY:
        {
            ASTPtr ast;
            String database, table_name;
            bool any_database, any_table;
            if (ParserRowPolicyNames{}.parse(pos, ast, expected))
                row_policy_names = typeid_cast<std::shared_ptr<ASTRowPolicyNames>>(ast);
            else if (parseOnDBAndTableName(pos, expected, database, any_database, table_name, any_table))
            {
                if (any_database)
                    all = true;
                else
                    database_and_table_name.emplace(database, table_name);
            }
            else if (parseIdentifierOrStringLiteral(pos, expected, short_name))
            {
            }
            else if (plural)
                all = true;
            else
                return false;
            break;
        }
        case EntityType::SETTINGS_PROFILE:
        {
            if (parseIdentifiersOrStringLiterals(pos, expected, names))
            {
            }
            else if (plural)
                all = true;
            else
                return false;
            break;
        }
        case EntityType::QUOTA:
        {
            if (parseIdentifiersOrStringLiterals(pos, expected, names))
            {
            }
            else if (plural)
                all = true;
            else
                current_quota = true;
            break;
        }
        case EntityType::MAX:
            throw Exception("Type " + toString(type) + " is not implemented in SHOW CREATE query", ErrorCodes::NOT_IMPLEMENTED);
    }

    auto query = std::make_shared<ASTShowCreateAccessEntityQuery>();
    node = query;

    query->type = type;
    query->names = std::move(names);
    query->current_quota = current_quota;
    query->current_user = current_user;
    query->row_policy_names = std::move(row_policy_names);
    query->all = all;
    query->short_name = std::move(short_name);
    query->database_and_table_name = std::move(database_and_table_name);

    return true;
}
}
