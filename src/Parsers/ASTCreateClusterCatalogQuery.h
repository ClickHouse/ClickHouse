#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <Common/SettingsChanges.h>

#include <vector>


namespace DB
{

/// `CREATE CLUSTER` / `CREATE SHARD` — adds a SQL cluster catalog entry.
/// The target kind is distinguished by `Kind`; `members` holds shard members (for `Cluster`)
/// or replica named collections (for `Shard`).
class ASTCreateClusterCatalogQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    enum class Kind : uint8_t
    {
        Cluster,
        Shard,
    };

    Kind kind = Kind::Cluster;
    String name;
    /// For `Cluster` — shard member names (existing `SHARD` definitions or whole-shard named collections).
    /// For `Shard` — replica named collection names.
    std::vector<String> members;
    /// Optional `PROPERTIES (...)`; syntax only at parse time. Semantics validated in interpreter.
    SettingsChanges properties;
    bool if_not_exists = false;
    /// After `ON CLUSTER ...`, optional `SYNC` (wait for distributed DDL when task timeout would otherwise skip it).
    bool sync = false;

    String getID(char) const override
    {
        return kind == Kind::Cluster ? "CreateClusterQuery" : "CreateShardQuery";
    }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTCreateClusterCatalogQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
