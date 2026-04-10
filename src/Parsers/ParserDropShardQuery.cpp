#include <Parsers/ASTDropShardQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropShardQuery.h>


namespace DB
{

bool ParserDropShardQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_shard(Keyword::SHARD);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    if (!s_drop.ignore(pos, expected))
        return false;
    if (!s_shard.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr shard_ast;
    if (!name_p.parse(pos, shard_ast, expected))
        return false;

    String cluster_str;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = make_intrusive<ASTDropShardQuery>();
    tryGetIdentifierNameInto(shard_ast, query->shard_name);
    query->if_exists = if_exists;
    query->cluster = std::move(cluster_str);
    node = query;
    return true;
}

}
