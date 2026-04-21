#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

/// `DROP CLUSTER` / `DROP SHARD` — removes a SQL cluster catalog entry previously created with
/// `CREATE CLUSTER` or `CREATE SHARD`. The target kind is distinguished by `Kind`.
class ASTDropClusterCatalogQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    enum class Kind : uint8_t
    {
        Cluster,
        Shard,
    };

    Kind kind = Kind::Cluster;
    String name;
    bool if_exists = false;

    String getID(char) const override
    {
        return kind == Kind::Cluster ? "DropClusterQuery" : "DropShardQuery";
    }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTDropClusterCatalogQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
