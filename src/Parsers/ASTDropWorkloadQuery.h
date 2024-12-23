#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTDropWorkloadQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String workload_name;

    bool if_exists = false;

    String getID(char) const override { return "DropWorkloadQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTDropWorkloadQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Drop; }
};

}
