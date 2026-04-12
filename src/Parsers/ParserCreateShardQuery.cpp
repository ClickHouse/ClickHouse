#include <Parsers/ASTCreateShardQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateShardQuery.h>
#include <Parsers/ParserSQLClusterCatalogProperties.h>
#include <Parsers/ParserSQLClusterShardReplicaList.h>


namespace DB
{

bool ParserCreateShardQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_shard(Keyword::SHARD);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    if (!s_create.ignore(pos, expected))
        return false;
    if (!s_shard.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    String shard_name;
    std::vector<String> replicas;

    ASTPtr shard_ast;
    if (!name_p.parse(pos, shard_ast, expected))
        return false;
    tryGetIdentifierNameInto(shard_ast, shard_name);

    if (!parseSQLClusterShardReplicaCollectionList(replicas, pos, expected))
        return false;

    SettingsChanges shard_properties;
    bool parsed_options [[maybe_unused]] = false;
    if (!parseSQLClusterCatalogOptionalProperties(shard_properties, parsed_options, pos, expected))
        return false;

    String cluster_str;
    bool sync = false;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
        ParserKeyword s_sync(Keyword::SYNC);
        if (s_sync.ignore(pos, expected))
            sync = true;
    }

    auto query = make_intrusive<ASTCreateShardQuery>();
    query->shard_name = std::move(shard_name);
    query->replicas = std::move(replicas);
    query->shard_properties = std::move(shard_properties);
    query->if_not_exists = if_not_exists;
    query->cluster = std::move(cluster_str);
    query->sync = sync;
    node = query;
    return true;
}

}
