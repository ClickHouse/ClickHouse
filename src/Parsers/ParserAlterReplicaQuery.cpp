#include <Parsers/ASTAlterReplicaQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserAlterReplicaQuery.h>
#include <Parsers/ParserSQLClusterCatalogProperties.h>

namespace DB
{

bool ParserAlterReplicaQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter(Keyword::ALTER);
    ParserKeyword s_replica(Keyword::REPLICA);
    ParserKeyword s_modify(Keyword::MODIFY);
    ParserKeyword s_properties(Keyword::PROPERTIES);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    if (!s_alter.ignore(pos, expected))
        return false;
    if (!s_replica.ignore(pos, expected))
        return false;

    ASTPtr replica_ast;
    if (!name_p.parse(pos, replica_ast, expected))
        return false;
    if (!s_modify.ignore(pos, expected))
        return false;
    if (!s_properties.ignore(pos, expected))
        return false;

    SettingsChanges properties;
    if (!parseSQLClusterCatalogPropertiesAssignments(properties, pos, expected))
        return false;
    if (properties.empty())
        return false;

    String cluster_str;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = make_intrusive<ASTAlterReplicaQuery>();
    tryGetIdentifierNameInto(replica_ast, query->replica_name);
    query->properties = std::move(properties);
    query->cluster = std::move(cluster_str);
    node = query;
    return true;
}

}
