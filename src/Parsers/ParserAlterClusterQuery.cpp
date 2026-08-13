#include <Parsers/ASTAlterClusterQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserAlterClusterQuery.h>
#include <Parsers/ParserSQLClusterAlterReplaceList.h>
#include <Parsers/ParserSQLClusterCatalogProperties.h>
#include <Parsers/ParserSQLClusterCatalogSyncTail.h>


namespace DB
{

bool ParserAlterClusterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter(Keyword::ALTER);
    ParserKeyword s_cluster(Keyword::CLUSTER);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_add(Keyword::ADD);
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_modify(Keyword::MODIFY);
    ParserKeyword s_rename(Keyword::RENAME);
    ParserKeyword s_replace(Keyword::REPLACE);
    ParserKeyword s_shard(Keyword::SHARD);
    ParserKeyword s_to(Keyword::TO);
    ParserKeyword s_properties(Keyword::PROPERTIES);
    ParserIdentifier name_p;
    ParserToken s_comma(TokenType::Comma);

    if (!s_alter.ignore(pos, expected))
        return false;
    if (!s_cluster.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr cluster_ast;
    if (!name_p.parse(pos, cluster_ast, expected))
        return false;

    String cluster_name;
    tryGetIdentifierNameInto(cluster_ast, cluster_name);

    ParserKeyword s_on(Keyword::ON);
    auto finish = [&](ASTPtr query) -> bool
    {
        auto & alter = query->as<ASTAlterClusterQuery &>();
        alter.cluster_name = cluster_name;
        alter.if_exists = if_exists;
        if (s_on.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, alter.cluster, expected))
                return false;
        }
        if (!parseSQLClusterCatalogSyncTail(alter.sync, pos, expected))
            return false;
        node = std::move(query);
        return true;
    };

    if (s_add.ignore(pos, expected))
    {
        if (!s_shard.ignore(pos, expected))
            return false;

        std::vector<String> shards_to_add;
        ASTPtr id_ast;
        if (!name_p.parse(pos, id_ast, expected))
            return false;
        tryGetIdentifierNameInto(id_ast, shards_to_add.emplace_back());
        while (s_comma.ignore(pos, expected))
        {
            if (!name_p.parse(pos, id_ast, expected))
                return false;
            tryGetIdentifierNameInto(id_ast, shards_to_add.emplace_back());
        }

        auto query = make_intrusive<ASTAlterClusterQuery>();
        query->command = AlterClusterCommand::AddShard;
        query->add_shard_members = std::move(shards_to_add);
        return finish(std::move(query));
    }

    if (s_drop.ignore(pos, expected))
    {
        if (!s_shard.ignore(pos, expected))
            return false;

        std::vector<String> shards_to_drop;
        ASTPtr id_ast;
        if (!name_p.parse(pos, id_ast, expected))
            return false;
        tryGetIdentifierNameInto(id_ast, shards_to_drop.emplace_back());
        while (s_comma.ignore(pos, expected))
        {
            if (!name_p.parse(pos, id_ast, expected))
                return false;
            tryGetIdentifierNameInto(id_ast, shards_to_drop.emplace_back());
        }

        auto query = make_intrusive<ASTAlterClusterQuery>();
        query->command = AlterClusterCommand::DropShard;
        query->drop_shard_members = std::move(shards_to_drop);
        return finish(std::move(query));
    }

    if (s_modify.ignore(pos, expected))
    {
        if (!s_shard.ignore(pos, expected))
            return false;

        ASTPtr mod_ast;
        if (!name_p.parse(pos, mod_ast, expected))
            return false;

        SettingsChanges modify_shard_properties;
        bool parsed_options = false;
        if (!parseSQLClusterCatalogOptionalProperties(modify_shard_properties, parsed_options, pos, expected))
            return false;
        if (!parsed_options)
            return false;

        auto query = make_intrusive<ASTAlterClusterQuery>();
        query->command = AlterClusterCommand::ModifyShard;
        tryGetIdentifierNameInto(mod_ast, query->modify_shard_name);
        query->modify_shard_properties = std::move(modify_shard_properties);
        return finish(std::move(query));
    }

    if (s_rename.ignore(pos, expected))
    {
        if (!s_shard.ignore(pos, expected))
            return false;

        ASTPtr from_ast;
        if (!name_p.parse(pos, from_ast, expected))
            return false;
        if (!s_to.ignore(pos, expected))
            return false;
        ASTPtr to_ast;
        if (!name_p.parse(pos, to_ast, expected))
            return false;

        auto query = make_intrusive<ASTAlterClusterQuery>();
        query->command = AlterClusterCommand::RenameShard;
        tryGetIdentifierNameInto(from_ast, query->rename_shard_from);
        tryGetIdentifierNameInto(to_ast, query->rename_shard_to);
        return finish(std::move(query));
    }

    if (s_replace.ignore(pos, expected))
    {
        auto query = make_intrusive<ASTAlterClusterQuery>();
        query->command = AlterClusterCommand::ReplaceClusterMembers;

        while (true)
        {
            AlterClusterMemberReplaceClause clause;
            if (!parseSQLClusterReplaceList(clause.from_members, pos, expected, false))
                return false;
            if (!s_to.ignore(pos, expected))
                return false;
            if (!parseSQLClusterReplaceList(clause.to_members, pos, expected, true))
                return false;
            if (clause.from_members.size() != clause.to_members.size())
                return false;
            query->member_replace_clauses.push_back(std::move(clause));

            if (!s_comma.ignore(pos, expected))
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
            if (!parseSQLClusterCatalogPropertiesAssignments(query->cluster_definition_properties, pos, expected))
                return false;
        }
        return finish(std::move(query));
    }

    return false;
}

}
