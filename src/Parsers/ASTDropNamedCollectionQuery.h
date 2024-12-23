#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTDropNamedCollectionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    std::string collection_name;
    bool if_exists = false;

    String getID(char) const override { return "DropNamedCollectionQuery"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTDropNamedCollectionQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Drop; }
};

}
