#include <Parsers/Access/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/Access/ParserRowPolicyName.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <base/range.h>
#include <cassert>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace
{
    bool parseEntityType(IParserBase::Pos & pos, Expected & expected, AccessEntityType & type, bool & plural)
    {
        for (auto i : collections::range(AccessEntityType::MAX))
        {
            const auto & type_info = AccessEntityTypeInfo::get(i);
            if (ParserKeyword::createDeprecated(type_info.name).ignore(pos, expected)
                || (!type_info.alias.empty() && ParserKeyword::createDeprecated(type_info.alias).ignore(pos, expected)))
            {
                type = i;
                plural = false;
                return true;
            }
        }

        for (auto i : collections::range(AccessEntityType::MAX))
        {
            const auto & type_info = AccessEntityTypeInfo::get(i);
            if (ParserKeyword::createDeprecated(type_info.plural_name).ignore(pos, expected)
                || (!type_info.plural_alias.empty() && ParserKeyword::createDeprecated(type_info.plural_alias).ignore(pos, expected)))
            {
                type = i;
                plural = true;
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


bool ParserShowCreateAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::SHOW_CREATE}.ignore(pos, expected))
        return false;

    AccessEntityType type;
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
        case AccessEntityType::USER:
        {
            if (parseCurrentUserTag(pos, expected))
                current_user = true;
            else if (parseUserNames(pos, expected, names, /*allow_query_parameter=*/ false))
            {
            }
            else if (plural)
                all = true;
            else
                current_user = true;
            break;
        }
        case AccessEntityType::ROLE:
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
        case AccessEntityType::ROW_POLICY:
        {
            ASTPtr ast;
            String database;
            String table_name;
            bool wildcard = false;
            bool default_database = false;
            if (ParserRowPolicyNames{}.parse(pos, ast, expected))
                row_policy_names = typeid_cast<std::shared_ptr<ASTRowPolicyNames>>(ast);
            else if (parseOnDBAndTableName(pos, expected, database, table_name, wildcard, default_database))
            {
                if (database.empty() && !default_database)
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
        case AccessEntityType::SETTINGS_PROFILE:
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
        case AccessEntityType::QUOTA:
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
        case AccessEntityType::MAX:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type {} is not implemented in SHOW CREATE query", toString(type));
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
