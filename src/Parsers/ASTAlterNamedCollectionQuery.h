#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Common/SettingsChanges.h>


namespace DB
{

class ASTAlterNamedCollectionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    std::string collection_name;
    SettingsChanges changes;
    std::vector<std::string> delete_keys;
    bool if_exists = false;
    std::unordered_map<String, bool> overridability;

    String getID(char) const override { return "AlterNamedCollectionQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTAlterNamedCollectionQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Alter; }
};

}
