#include <Parsers/ASTAlterShardQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserAlterShardQuery.h>
#include <Parsers/ParserSQLClusterAlterReplaceList.h>
#include <Parsers/ParserSQLClusterCatalogProperties.h>


namespace DB
{

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
            if (!parseSQLClusterReplaceList(clause.from_collections, pos, expected, false))
                return false;
            if (!s_to.ignore(pos, expected))
                return false;
            if (!parseSQLClusterReplaceList(clause.to_collections, pos, expected, true))
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
        /// `ADD REPLICA` only attaches an existing replica named collection (one created via `CREATE REPLICA`)
        /// to the shard — it does NOT mutate the collection's own properties. Use `ALTER REPLICA name
        /// PROPERTIES (...)` for that.
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
