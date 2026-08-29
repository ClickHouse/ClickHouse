#include <Parsers/ASTSQLClusterQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSQLClusterQuery.h>


namespace DB
{

namespace
{

bool parsePropertiesList(SettingsChanges & properties, IParser::Pos & pos, Expected & expected)
{
    ParserToken s_comma(TokenType::Comma);

    while (true)
    {
        if (!properties.empty() && !s_comma.ignore(pos, expected))
            break;

        properties.push_back(SettingChange{});
        if (!ParserSetQuery::parseNameValuePair(properties.back(), pos, expected))
            return false;
    }

    return true;
}

bool parseReplica(IParser::Pos & pos, Expected & expected, ASTPtr & replica)
{
    ParserKeyword s_replica(Keyword::REPLICA);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);

    if (!s_replica.ignore(pos, expected))
        return false;
    if (!s_lparen.ignore(pos, expected))
        return false;

    auto replica_ast = make_intrusive<ASTSQLClusterReplica>();
    if (!parsePropertiesList(replica_ast->properties, pos, expected))
        return false;

    if (!s_rparen.ignore(pos, expected))
        return false;

    replica = replica_ast;
    return true;
}

bool parseShard(IParser::Pos & pos, Expected & expected, ASTPtr & shard)
{
    ParserKeyword s_shard(Keyword::SHARD);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserToken s_comma(TokenType::Comma);

    if (!s_shard.ignore(pos, expected))
        return false;
    if (!s_lparen.ignore(pos, expected))
        return false;

    auto shard_ast = make_intrusive<ASTSQLClusterShard>();

    while (!pos.isEnd())
    {
        if (s_rparen.ignore(pos, expected))
            break;

        if (!shard_ast->properties.empty() || !shard_ast->replicas.empty())
        {
            if (!s_comma.ignore(pos, expected))
                return false;
        }

        ASTPtr replica;
        if (parseReplica(pos, expected, replica))
        {
            shard_ast->replicas.push_back(replica);
            continue;
        }

        if (!shard_ast->replicas.empty())
            return false;

        shard_ast->properties.push_back(SettingChange{});
        if (!ParserSetQuery::parseNameValuePair(shard_ast->properties.back(), pos, expected))
            return false;
    }

    shard = shard_ast;
    return true;
}

bool parseClusterDefinition(IParser::Pos & pos, Expected & expected, ASTPtr & definition)
{
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserToken s_comma(TokenType::Comma);

    if (!s_lparen.ignore(pos, expected))
        return false;

    auto definition_ast = make_intrusive<ASTSQLClusterDefinition>();

    while (!pos.isEnd())
    {
        if (s_rparen.ignore(pos, expected))
            break;

        if (!definition_ast->cluster_properties.empty() || !definition_ast->shards.empty())
        {
            if (!s_comma.ignore(pos, expected))
                return false;
        }

        ASTPtr shard;
        if (parseShard(pos, expected, shard))
        {
            definition_ast->shards.push_back(shard);
            continue;
        }

        if (!definition_ast->shards.empty())
            return false;

        definition_ast->cluster_properties.push_back(SettingChange{});
        if (!ParserSetQuery::parseNameValuePair(definition_ast->cluster_properties.back(), pos, expected))
            return false;
    }

    if (definition_ast->shards.empty())
        return false;

    definition = definition_ast;
    return true;
}

}

bool ParserCreateSQLClusterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_cluster(Keyword::CLUSTER);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserIdentifier name_p;

    if (!s_create.ignore(pos, expected))
        return false;
    if (!s_cluster.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;

    ASTPtr definition;
    if (!parseClusterDefinition(pos, expected, definition))
        return false;

    auto query = make_intrusive<ASTCreateSQLClusterQuery>();
    tryGetIdentifierNameInto(name_ast, query->cluster_name);
    query->definition = definition;
    query->if_not_exists = if_not_exists;
    node = query;
    return true;
}

bool ParserAlterSQLClusterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter(Keyword::ALTER);
    ParserKeyword s_cluster(Keyword::CLUSTER);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserIdentifier name_p;

    if (!s_alter.ignore(pos, expected))
        return false;
    if (!s_cluster.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;

    ASTPtr definition;
    if (!parseClusterDefinition(pos, expected, definition))
        return false;

    auto query = make_intrusive<ASTAlterSQLClusterQuery>();
    tryGetIdentifierNameInto(name_ast, query->cluster_name);
    query->definition = definition;
    query->if_exists = if_exists;
    node = query;
    return true;
}

bool ParserDropSQLClusterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_cluster(Keyword::CLUSTER);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserIdentifier name_p;

    if (!s_drop.ignore(pos, expected))
        return false;
    if (!s_cluster.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;

    auto query = make_intrusive<ASTDropSQLClusterQuery>();
    tryGetIdentifierNameInto(name_ast, query->cluster_name);
    query->if_exists = if_exists;
    node = query;
    return true;
}

}
