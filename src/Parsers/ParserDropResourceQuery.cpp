#include <Parsers/ASTDropResourceQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropResourceQuery.h>

namespace DB
{

bool ParserDropResourceQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_resource(Keyword::RESOURCE);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier resource_name_p;

    String cluster_str;
    bool if_exists = false;

    ASTPtr resource_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_resource.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!resource_name_p.parse(pos, resource_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto drop_resource_query = std::make_shared<ASTDropResourceQuery>();
    drop_resource_query->if_exists = if_exists;
    drop_resource_query->cluster = std::move(cluster_str);

    node = drop_resource_query;

    drop_resource_query->resource_name = resource_name->as<ASTIdentifier &>().name();

    return true;
}

}
