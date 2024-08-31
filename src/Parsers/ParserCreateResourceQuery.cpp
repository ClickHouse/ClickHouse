#include <Parsers/ParserCreateResourceQuery.h>

#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

bool ParserCreateResourceQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_resource(Keyword::RESOURCE);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier resource_name_p;
    // TODO(serxa): parse resource definition

    ASTPtr resource_name;

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (!s_resource.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!resource_name_p.parse(pos, resource_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto create_resource_query = std::make_shared<ASTCreateResourceQuery>();
    node = create_resource_query;

    create_resource_query->resource_name = resource_name;
    create_resource_query->children.push_back(resource_name);

    create_resource_query->or_replace = or_replace;
    create_resource_query->if_not_exists = if_not_exists;
    create_resource_query->cluster = std::move(cluster_str);

    return true;
}

}
