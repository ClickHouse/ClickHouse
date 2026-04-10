#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

/// `DROP CLUSTER` — removes a cluster definition created with `CREATE CLUSTER`.
class ASTDropClusterQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String cluster_name;
    bool if_exists = false;

    String getID(char) const override { return "DropClusterQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTDropClusterQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
