#include <Parsers/ASTCreateShardQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateShardQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Core/Field.h>
#include <Common/SettingsChanges.h>


namespace DB
{

bool ParserCreateShardQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_shard(Keyword::SHARD);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;
    ParserToken s_comma(TokenType::Comma);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);

    if (!s_create.ignore(pos, expected))
        return false;
    if (!s_shard.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    ASTPtr shard_ast;
    if (!name_p.parse(pos, shard_ast, expected))
        return false;

    if (!s_lparen.ignore(pos, expected))
        return false;

    std::vector<String> replicas;
    ASTPtr replica_id;
    if (!name_p.parse(pos, replica_id, expected))
        return false;
    tryGetIdentifierNameInto(replica_id, replicas.emplace_back());

    while (s_comma.ignore(pos, expected))
    {
        if (!name_p.parse(pos, replica_id, expected))
            return false;
        tryGetIdentifierNameInto(replica_id, replicas.emplace_back());
    }

    if (!s_rparen.ignore(pos, expected))
        return false;

    UInt32 weight = 1;
    bool internal_replication = false;

    if (s_settings.ignore(pos, expected))
    {
        SettingsChanges changes;
        while (true)
        {
            if (!changes.empty() && !s_comma.ignore(pos, expected))
                break;
            changes.push_back(SettingChange{});
            if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
                return false;
        }
        for (const auto & ch : changes)
        {
            if (ch.name == "weight")
                weight = static_cast<UInt32>(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), ch.value));
            else if (ch.name == "internal_replication")
            {
                if (ch.value.getType() == Field::Types::Bool)
                    internal_replication = ch.value.safeGet<bool>();
                else
                    internal_replication = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), ch.value) != 0;
            }
            else
                return false;
        }
    }

    String cluster_str;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = make_intrusive<ASTCreateShardQuery>();
    tryGetIdentifierNameInto(shard_ast, query->shard_name);
    query->replicas = std::move(replicas);
    query->weight = weight;
    query->internal_replication = internal_replication;
    query->if_not_exists = if_not_exists;
    query->cluster = std::move(cluster_str);
    node = query;
    return true;
}

}
