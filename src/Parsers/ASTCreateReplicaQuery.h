#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <Common/SettingsChanges.h>


namespace DB
{

/// `CREATE REPLICA` — sugar for `CREATE NAMED COLLECTION` (SQL catalog connection parameters).
class ASTCreateReplicaQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String replica_name;
    SettingsChanges properties;
    bool if_not_exists = false;

    String getID(char) const override { return "CreateReplicaQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTCreateReplicaQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
