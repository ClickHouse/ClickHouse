#include <Parsers/ASTDropReplicaQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropReplicaQuery.h>

namespace DB
{

bool ParserDropReplicaQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_replica(Keyword::REPLICA);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    if (!s_drop.ignore(pos, expected))
        return false;
    if (!s_replica.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr replica_ast;
    if (!name_p.parse(pos, replica_ast, expected))
        return false;

    String cluster_str;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = make_intrusive<ASTDropReplicaQuery>();
    tryGetIdentifierNameInto(replica_ast, query->replica_name);
    query->if_exists = if_exists;
    query->cluster = std::move(cluster_str);
    node = query;
    return true;
}

}
