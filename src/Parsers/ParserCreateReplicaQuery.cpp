#include <Parsers/ASTCreateReplicaQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateReplicaQuery.h>
#include <Parsers/ParserSQLClusterCatalogProperties.h>


namespace DB
{

bool ParserCreateReplicaQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_replica(Keyword::REPLICA);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_properties(Keyword::PROPERTIES);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    if (!s_create.ignore(pos, expected))
        return false;
    if (!s_replica.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    ASTPtr replica_ast;
    if (!name_p.parse(pos, replica_ast, expected))
        return false;

    if (!s_properties.ignore(pos, expected))
        return false;

    SettingsChanges properties;
    if (!parseSQLClusterCatalogPropertiesAssignments(properties, pos, expected))
        return false;

    String cluster_str;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = make_intrusive<ASTCreateReplicaQuery>();
    tryGetIdentifierNameInto(replica_ast, query->replica_name);
    query->properties = std::move(properties);
    query->if_not_exists = if_not_exists;
    query->cluster = std::move(cluster_str);
    node = query;
    return true;
}

}
