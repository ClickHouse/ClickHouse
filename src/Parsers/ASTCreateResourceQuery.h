#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTCreateResourceQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr resource_name;
    // TODO(serxa): add resource definition

    bool or_replace = false;
    bool if_not_exists = false;

    String getID(char delim) const override { return "CreateResourceQuery" + (delim + getResourceName()); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateResourceQuery>(clone()); }

    String getResourceName() const;

    QueryKind getQueryKind() const override { return QueryKind::Create; }
};

}
