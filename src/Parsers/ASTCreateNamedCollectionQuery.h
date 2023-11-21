#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Common/SettingsChanges.h>


namespace DB
{

class ASTCreateNamedCollectionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    std::string collection_name;
    SettingsChanges changes;

    String getID(char) const override { return "CreateNamedCollectionQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateNamedCollectionQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Create; }

    std::string getCollectionName() const;
};

}
