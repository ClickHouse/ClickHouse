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
    ParserKeyword s_alter("ALTER");
    ParserKeyword s_collection("NAMED COLLECTION");
    ParserKeyword s_if_exists("IF EXISTS");
    ParserKeyword s_on("ON");
    ParserKeyword s_delete("DELETE");
    ParserIdentifier name_p;
    ParserSetQuery set_p;
    ParserToken s_comma(TokenType::Comma);

    String cluster_str;
    bool if_exists = false;

    ASTPtr collection_name;
    ASTPtr set;
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

    bool parsed_delete = false;
    if (!set_p.parse(pos, set, expected))
    {
        if (!s_delete.ignore(pos, expected))
            return false;

        parsed_delete = true;
    }
    else if (s_delete.ignore(pos, expected))
    {
        parsed_delete = true;
    }

    if (parsed_delete)
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
    if (set)
        query->changes = set->as<ASTSetQuery>()->changes;
    query->delete_keys = delete_keys;

    node = query;
    return true;
}

}
