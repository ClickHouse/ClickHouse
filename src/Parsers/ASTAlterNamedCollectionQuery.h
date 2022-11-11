#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTAlterNamedCollectionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    std::string collection_name;
    ASTPtr changes;
    bool if_exists = false;

    String getID(char) const override { return "AlterNamedCollectionQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTAlterNamedCollectionQuery>(clone()); }
};

}
