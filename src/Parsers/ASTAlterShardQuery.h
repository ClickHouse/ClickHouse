#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <Common/SettingsChanges.h>

#include <vector>


namespace DB
{

/// One `REPLACE ... TO ...` clause in `ALTER SHARD ... REPLACE ...` (lists must have equal length).
struct AlterShardReplicaReplaceClause
{
    std::vector<String> from_collections;
    std::vector<String> to_collections;
};

enum class AlterShardCommand : uint8_t
{
    /// `ALTER SHARD name MODIFY PROPERTIES (...)` — shard-level options only; replica list unchanged.
    ModifyShardProperties,
    /// `ALTER SHARD name REPLACE ... TO ... [, REPLACE ... TO ...] [MODIFY PROPERTIES (...)]` (optional)
    ReplaceReplicas,
    AddReplica,
    DropReplica,
    ModifyReplica,
    RenameReplica,
};

/// `ALTER SHARD` — SQL catalog shard: shard-level `MODIFY PROPERTIES` or replica-level subcommands (see `AlterShardCommand`).
class ASTAlterShardQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    AlterShardCommand command = AlterShardCommand::ModifyShardProperties;

    String shard_name;
    bool if_exists = false;

    /// `ModifyShardProperties` / optional tail of `ReplaceReplicas` — shard-level `MODIFY PROPERTIES`.
    SettingsChanges shard_definition_properties;

    /// `ReplaceReplicas` — one or more `REPLACE from... TO to...` clauses.
    std::vector<AlterShardReplicaReplaceClause> replica_replace_clauses;

    /// `AddReplica` / `DropReplica` / `ModifyReplica` — target collection name; `RenameReplica` — old name.
    String replica_name;
    /// `RenameReplica` — new collection name.
    String rename_replica_to;
    /// `AddReplica` / `ModifyReplica` — optional per-replica parameters (validated later).
    SettingsChanges replica_properties;

    String getID(char) const override { return "AlterShardQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTAlterShardQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Alter; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
