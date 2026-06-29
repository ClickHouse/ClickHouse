#include <Parsers/ASTCreateClusterCatalogQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateClusterCatalogQuery.h>
#include <Parsers/ParserSQLClusterCatalogProperties.h>
#include <Parsers/ParserSQLClusterShardReplicaList.h>


namespace DB
{

namespace
{

/// Shared tail: `[ON CLUSTER <cluster> [SYNC]]`.
bool parseOnClusterTail(String & cluster_str, bool & sync, IParser::Pos & pos, Expected & expected)
{
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_sync(Keyword::SYNC);

    if (!s_on.ignore(pos, expected))
        return true;

    if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
        return false;
    if (s_sync.ignore(pos, expected))
        sync = true;
    return true;
}

bool parseClusterMemberList(std::vector<String> & members, IParser::Pos & pos, Expected & expected)
{
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserToken s_comma(TokenType::Comma);
    ParserIdentifier name_p;

    if (!s_lparen.ignore(pos, expected))
        return false;

    ASTPtr member_id;
    if (!name_p.parse(pos, member_id, expected))
        return false;
    tryGetIdentifierNameInto(member_id, members.emplace_back());

    while (s_comma.ignore(pos, expected))
    {
        if (!name_p.parse(pos, member_id, expected))
            return false;
        tryGetIdentifierNameInto(member_id, members.emplace_back());
    }

    return s_rparen.ignore(pos, expected);
}

}

bool ParserCreateClusterCatalogQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_cluster(Keyword::CLUSTER);
    ParserKeyword s_shard(Keyword::SHARD);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserIdentifier name_p;

    if (!s_create.ignore(pos, expected))
        return false;

    ASTCreateClusterCatalogQuery::Kind kind;
    if (s_cluster.ignore(pos, expected))
        kind = ASTCreateClusterCatalogQuery::Kind::Cluster;
    else if (s_shard.ignore(pos, expected))
        kind = ASTCreateClusterCatalogQuery::Kind::Shard;
    else
        return false;

    bool if_not_exists = false;
    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;

    std::vector<String> members;
    if (kind == ASTCreateClusterCatalogQuery::Kind::Cluster)
    {
        if (!parseClusterMemberList(members, pos, expected))
            return false;
    }
    else
    {
        if (!parseSQLClusterShardReplicaCollectionList(members, pos, expected))
            return false;
    }

    SettingsChanges properties;
    bool parsed_options [[maybe_unused]] = false;
    if (!parseSQLClusterCatalogOptionalProperties(properties, parsed_options, pos, expected))
        return false;

    String cluster_str;
    bool sync = false;
    if (!parseOnClusterTail(cluster_str, sync, pos, expected))
        return false;

    auto query = make_intrusive<ASTCreateClusterCatalogQuery>();
    query->kind = kind;
    tryGetIdentifierNameInto(name_ast, query->name);
    query->members = std::move(members);
    query->properties = std::move(properties);
    query->if_not_exists = if_not_exists;
    query->cluster = std::move(cluster_str);
    query->sync = sync;
    node = query;
    return true;
}

}
