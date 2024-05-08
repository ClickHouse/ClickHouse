#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>

namespace DB
{

bool ParserDropNamedCollectionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_collection(Keyword::NAMED_COLLECTION);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    String cluster_str;
    bool if_exists = false;

    ASTPtr collection_name;

    if (!s_drop.ignore(pos, expected))
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

    auto query = std::make_shared<ASTDropNamedCollectionQuery>();

    tryGetIdentifierNameInto(collection_name, query->collection_name);
    query->if_exists = if_exists;
    query->cluster = std::move(cluster_str);

    node = query;
    return true;
}

}
