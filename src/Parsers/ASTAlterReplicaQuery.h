#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/IAST.h>

#include <Common/SettingsChanges.h>

namespace DB
{

class ASTAlterReplicaQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String replica_name;
    SettingsChanges properties;

    String getID(char) const override { return "AlterReplicaQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTAlterReplicaQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Alter; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
