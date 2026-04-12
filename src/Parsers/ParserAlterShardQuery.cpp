#include <Parsers/ASTAlterShardQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserAlterShardQuery.h>
#include <Parsers/ParserSQLClusterCatalogProperties.h>


namespace DB
{

namespace
{

/// One side of `REPLACE from... TO to...`: either `(id, id, ...)` or `id` / `id, id, ...` until `TO` (from side) or
/// until `REPLACE` / `MODIFY PROPERTIES` / `ON` after a comma (to side — so `..., REPLACE` is not parsed as a second to-id).
bool parseReplaceReplicaList(std::vector<String> & out, IParser::Pos & pos, Expected & expected, bool is_to_side)
{
    ParserToken s_comma(TokenType::Comma);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserIdentifier name_p;
    ParserKeyword s_to(Keyword::TO);
    ParserKeyword s_replace(Keyword::REPLACE);
    ParserKeyword s_properties(Keyword::PROPERTIES);
    ParserKeyword s_modify_kw(Keyword::MODIFY);
    ParserKeyword s_on(Keyword::ON);

    if (s_lparen.ignore(pos, expected))
    {
        ASTPtr id;
        if (!name_p.parse(pos, id, expected))
            return false;
        tryGetIdentifierNameInto(id, out.emplace_back());
        while (s_comma.ignore(pos, expected))
        {
            if (!name_p.parse(pos, id, expected))
                return false;
            tryGetIdentifierNameInto(id, out.emplace_back());
        }
        if (!s_rparen.ignore(pos, expected))
            return false;
        return !out.empty();
    }

    ASTPtr id;
    if (!name_p.parse(pos, id, expected))
        return false;
    tryGetIdentifierNameInto(id, out.emplace_back());

    while (true)
    {
        const auto before_comma = pos;
        if (!s_comma.ignore(pos, expected))
            break;
        if (s_to.check(pos, expected))
        {
            pos = before_comma;
            break;
        }
        if (is_to_side && (s_replace.check(pos, expected) || s_on.check(pos, expected)))
        {
            pos = before_comma;
            break;
        }
        if (is_to_side)
        {
            auto lookahead = pos;
            if (s_modify_kw.ignore(lookahead, expected) && s_properties.check(lookahead, expected))
            {
                pos = before_comma;
                break;
            }
        }
        if (!name_p.parse(pos, id, expected))
            return false;
        tryGetIdentifierNameInto(id, out.emplace_back());
    }
    return true;
}

}

bool ParserAlterShardQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter(Keyword::ALTER);
    ParserKeyword s_shard(Keyword::SHARD);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_add(Keyword::ADD);
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_modify(Keyword::MODIFY);
    ParserKeyword s_rename(Keyword::RENAME);
    ParserKeyword s_replica(Keyword::REPLICA);
    ParserKeyword s_to(Keyword::TO);
    ParserKeyword s_properties(Keyword::PROPERTIES);
    ParserKeyword s_replace(Keyword::REPLACE);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    if (!s_alter.ignore(pos, expected))
        return false;
    if (!s_shard.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr shard_ast;
    if (!name_p.parse(pos, shard_ast, expected))
        return false;

    String shard_name;
    tryGetIdentifierNameInto(shard_ast, shard_name);

    auto finish = [&](ASTPtr query)
    {
        auto & alter = query->as<ASTAlterShardQuery &>();
        alter.shard_name = shard_name;
        alter.if_exists = if_exists;
        node = std::move(query);
    };

    /// `ALTER SHARD name REPLACE ... TO ... [, REPLACE ... TO ...] [MODIFY PROPERTIES (...)]` — must parse before `MODIFY PROPERTIES` / `MODIFY REPLICA`.
    if (s_replace.ignore(pos, expected))
    {
        auto query = make_intrusive<ASTAlterShardQuery>();
        query->command = AlterShardCommand::ReplaceReplicas;

        while (true)
        {
            AlterShardReplicaReplaceClause clause;
            if (!parseReplaceReplicaList(clause.from_collections, pos, expected, false))
                return false;
            if (!s_to.ignore(pos, expected))
                return false;
            if (!parseReplaceReplicaList(clause.to_collections, pos, expected, true))
                return false;
            if (clause.from_collections.size() != clause.to_collections.size())
                return false;
            query->replica_replace_clauses.push_back(std::move(clause));

            if (!ParserToken(TokenType::Comma).ignore(pos, expected))
                break;
            if (!s_replace.ignore(pos, expected))
            {
                expected.add(pos, "REPLACE");
                return false;
            }
        }

        if (s_modify.ignore(pos, expected))
        {
            if (!s_properties.ignore(pos, expected))
            {
                expected.add(pos, "PROPERTIES");
                return false;
            }
            if (!parseSQLClusterCatalogPropertiesAssignments(query->shard_definition_properties, pos, expected))
                return false;
        }
        String cluster_str;
        if (s_on.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
            query->cluster = std::move(cluster_str);
        }
        finish(std::move(query));
        return true;
    }

    /// `ALTER SHARD name MODIFY PROPERTIES (...)` — shard-level options only (no `REPLICA` list).
    if (s_modify.ignore(pos, expected))
    {
        if (!s_properties.ignore(pos, expected))
        {
            if (!s_replica.ignore(pos, expected))
                return false;
            ASTPtr rep_ast;
            if (!name_p.parse(pos, rep_ast, expected))
                return false;
            if (!s_properties.ignore(pos, expected))
                return false;
            auto query = make_intrusive<ASTAlterShardQuery>();
            query->command = AlterShardCommand::ModifyReplica;
            tryGetIdentifierNameInto(rep_ast, query->replica_name);
            if (!parseSQLClusterCatalogPropertiesAssignments(query->replica_properties, pos, expected))
                return false;
            String cluster_str;
            if (s_on.ignore(pos, expected))
            {
                if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                    return false;
                query->cluster = std::move(cluster_str);
            }
            finish(std::move(query));
            return true;
        }
        auto query = make_intrusive<ASTAlterShardQuery>();
        query->command = AlterShardCommand::ModifyShardProperties;
        if (!parseSQLClusterCatalogPropertiesAssignments(query->shard_definition_properties, pos, expected))
            return false;
        String cluster_str;
        if (s_on.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
            query->cluster = std::move(cluster_str);
        }
        finish(std::move(query));
        return true;
    }

    /// Subcommands: `ADD|DROP|MODIFY|RENAME REPLICA ...`
    if (s_add.ignore(pos, expected))
    {
        if (!s_replica.ignore(pos, expected))
            return false;
        ASTPtr rep_ast;
        if (!name_p.parse(pos, rep_ast, expected))
            return false;
        auto query = make_intrusive<ASTAlterShardQuery>();
        query->command = AlterShardCommand::AddReplica;
        tryGetIdentifierNameInto(rep_ast, query->replica_name);
        if (s_properties.ignore(pos, expected))
        {
            if (!parseSQLClusterCatalogPropertiesAssignments(query->replica_properties, pos, expected))
                return false;
        }
        String cluster_str;
        if (s_on.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
            query->cluster = std::move(cluster_str);
        }
        finish(std::move(query));
        return true;
    }

    if (s_drop.ignore(pos, expected))
    {
        if (!s_replica.ignore(pos, expected))
            return false;
        ASTPtr rep_ast;
        if (!name_p.parse(pos, rep_ast, expected))
            return false;
        auto query = make_intrusive<ASTAlterShardQuery>();
        query->command = AlterShardCommand::DropReplica;
        tryGetIdentifierNameInto(rep_ast, query->replica_name);
        String cluster_str;
        if (s_on.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
            query->cluster = std::move(cluster_str);
        }
        finish(std::move(query));
        return true;
    }

    if (s_rename.ignore(pos, expected))
    {
        if (!s_replica.ignore(pos, expected))
            return false;
        ASTPtr from_ast;
        if (!name_p.parse(pos, from_ast, expected))
            return false;
        if (!s_to.ignore(pos, expected))
            return false;
        ASTPtr to_ast;
        if (!name_p.parse(pos, to_ast, expected))
            return false;
        auto query = make_intrusive<ASTAlterShardQuery>();
        query->command = AlterShardCommand::RenameReplica;
        tryGetIdentifierNameInto(from_ast, query->replica_name);
        tryGetIdentifierNameInto(to_ast, query->rename_replica_to);
        String cluster_str;
        if (s_on.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
            query->cluster = std::move(cluster_str);
        }
        finish(std::move(query));
        return true;
    }

    return false;
}

}
