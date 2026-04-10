#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <vector>


namespace DB
{

/// `CREATE CLUSTER` — named cluster built from `SHARD` definitions and/or whole-shard named collections.
class ASTCreateClusterQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String cluster_name;
    std::vector<String> members;
    bool if_not_exists = false;

    String getID(char) const override { return "CreateClusterQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTCreateClusterQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
