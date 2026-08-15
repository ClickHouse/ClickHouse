#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserAlterNamedCollectionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

bool ParserAlterNamedCollectionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter(Keyword::ALTER);
    ParserKeyword s_collection(Keyword::NAMED_COLLECTION);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_delete(Keyword::DELETE);

    ParserIdentifier name_p;
    ParserKeyword s_set(Keyword::SET);
    ParserKeyword s_overridable(Keyword::OVERRIDABLE);
    ParserKeyword s_not_overridable(Keyword::NOT_OVERRIDABLE);
    ParserToken s_comma(TokenType::Comma);

    String cluster_str;
    bool if_exists = false;

    ASTPtr collection_name;

    std::vector<std::string> delete_keys;

    if (!s_alter.ignore(pos, expected))
        return false;

    if (!s_collection.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!name_p.parse(pos, collection_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    SettingsChanges changes;
    std::unordered_map<String, bool> overridability;
    if (s_set.ignore(pos, expected))
    {
        while (true)
        {
            if (!changes.empty() && !s_comma.ignore(pos))
                break;

            changes.push_back(SettingChange{});

            if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
                return false;
            if (s_not_overridable.ignore(pos, expected))
                overridability.emplace(changes.back().name, false);
            else if (s_overridable.ignore(pos, expected))
                overridability.emplace(changes.back().name, true);
        }
    }

    if (s_delete.ignore(pos, expected))
    {
        while (true)
        {
            if (!delete_keys.empty() && !s_comma.ignore(pos))
                break;

            ASTPtr key;
            if (!name_p.parse(pos, key, expected))
                return false;

            delete_keys.push_back(getIdentifierName(key));
        }
    }

    auto query = std::make_shared<ASTAlterNamedCollectionQuery>();

    query->collection_name = getIdentifierName(collection_name);
    query->if_exists = if_exists;
    query->cluster = std::move(cluster_str);
    query->changes = changes;
    query->overridability = std::move(overridability);
    query->delete_keys = delete_keys;

    node = query;
    return true;
}

}
