#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserAlterNamedCollectionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>

namespace DB
{

bool ParserAlterNamedCollectionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter("ALTER");
    ParserKeyword s_collection("NAMED COLLECTION");

    ParserIdentifier name_p;
    ParserSetQuery set_p;

    String cluster_str;
    bool if_exists = false;

    ASTPtr collection_name;
    ASTPtr changes;

    if (!s_alter.ignore(pos, expected))
        return false;

    if (!s_collection.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, collection_name, expected))
        return false;

    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (!set_p.parse(pos, changes, expected))
        return false;

    auto query = std::make_shared<ASTAlterNamedCollectionQuery>();

    tryGetIdentifierNameInto(collection_name, query->collection_name);
    query->if_exists = if_exists;
    query->cluster = std::move(cluster_str);
    query->changes = changes;

    node = query;
    return true;
}

}
