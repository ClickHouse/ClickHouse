#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTCreateWorkloadQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr workload_name;
    ASTPtr workload_parent;
    // TODO(serxa): add workload settings (weight and priority should also go inside settings, because they can differ for different resources)

    bool or_replace = false;
    bool if_not_exists = false;

    String getID(char delim) const override { return "CreateWorkloadQuery" + (delim + getWorkloadName()); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateWorkloadQuery>(clone()); }

    String getWorkloadName() const;
    bool hasParent() const;
    String getWorkloadParent() const;

    QueryKind getQueryKind() const override { return QueryKind::Create; }
};

}
