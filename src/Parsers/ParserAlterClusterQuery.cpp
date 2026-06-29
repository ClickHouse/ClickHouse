#include <Parsers/ASTAlterClusterQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserAlterClusterQuery.h>
#include <Parsers/ParserSQLClusterAlterReplaceList.h>
#include <Parsers/ParserSQLClusterCatalogProperties.h>


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
    ParserKeyword s_on(Keyword::ON);
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

    auto parse_on_cluster = [&](ASTAlterClusterQuery & q) -> bool
    {
        String cluster_str;
        if (s_on.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
            q.cluster = std::move(cluster_str);
        }
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
        query->cluster_name = std::move(cluster_name);
        query->add_shard_members = std::move(shards_to_add);
        query->if_exists = if_exists;
        if (!parse_on_cluster(*query))
            return false;
        node = std::move(query);
        return true;
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
        query->cluster_name = std::move(cluster_name);
        query->drop_shard_members = std::move(shards_to_drop);
        query->if_exists = if_exists;
        if (!parse_on_cluster(*query))
            return false;
        node = std::move(query);
        return true;
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
        query->cluster_name = std::move(cluster_name);
        tryGetIdentifierNameInto(mod_ast, query->modify_shard_name);
        query->modify_shard_properties = std::move(modify_shard_properties);
        query->if_exists = if_exists;
        if (!parse_on_cluster(*query))
            return false;
        node = std::move(query);
        return true;
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
        query->cluster_name = std::move(cluster_name);
        tryGetIdentifierNameInto(from_ast, query->rename_shard_from);
        tryGetIdentifierNameInto(to_ast, query->rename_shard_to);
        query->if_exists = if_exists;
        if (!parse_on_cluster(*query))
            return false;
        node = std::move(query);
        return true;
    }

    if (s_replace.ignore(pos, expected))
    {
        auto query = make_intrusive<ASTAlterClusterQuery>();
        query->command = AlterClusterCommand::ReplaceClusterMembers;
        query->cluster_name = cluster_name;
        query->if_exists = if_exists;

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
        if (!parse_on_cluster(*query))
            return false;
        node = std::move(query);
        return true;
    }

    return false;
}

}
