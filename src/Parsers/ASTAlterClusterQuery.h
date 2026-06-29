#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <Common/SettingsChanges.h>

#include <vector>


namespace DB
{

/// One `REPLACE ... TO ...` clause in `ALTER CLUSTER ... REPLACE ...` (lists must have equal length).
struct AlterClusterMemberReplaceClause
{
    std::vector<String> from_members;
    std::vector<String> to_members;
};

enum class AlterClusterCommand : uint8_t
{
    /// `ALTER CLUSTER name ADD SHARD s1, s2, ...` — append members.
    AddShard,
    /// `ALTER CLUSTER name DROP SHARD s1, s2, ...` — remove members.
    DropShard,
    ModifyShard,
    RenameShard,
    /// `ALTER CLUSTER name REPLACE ... TO ... [, REPLACE ...]` — remap members; optional trailing cluster `MODIFY PROPERTIES`.
    ReplaceClusterMembers,
};

/// `ALTER CLUSTER` — SQL catalog cluster: `ADD|DROP SHARD` membership edits, `REPLACE … TO …` remap (optional cluster `MODIFY PROPERTIES`), or `MODIFY|RENAME SHARD` (when implemented).
class ASTAlterClusterQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    AlterClusterCommand command = AlterClusterCommand::AddShard;

    String cluster_name;
    bool if_exists = false;

    /// `ADD SHARD` — one or more shard members (SQL shard name or whole-shard named collection).
    std::vector<String> add_shard_members;

    /// `DROP SHARD` — one or more members to remove.
    std::vector<String> drop_shard_members;

    /// `ReplaceClusterMembers` — one or more `REPLACE from... TO to...` clauses.
    std::vector<AlterClusterMemberReplaceClause> member_replace_clauses;
    /// `ReplaceClusterMembers` — optional cluster-level `MODIFY PROPERTIES` after all `REPLACE` clauses.
    SettingsChanges cluster_definition_properties;

    /// `MODIFY SHARD`
    String modify_shard_name;
    SettingsChanges modify_shard_properties;

    /// `RENAME SHARD`
    String rename_shard_from;
    String rename_shard_to;

    String getID(char) const override { return "AlterClusterQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTAlterClusterQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Alter; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
