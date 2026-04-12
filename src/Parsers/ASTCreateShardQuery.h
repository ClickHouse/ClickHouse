#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <Common/SettingsChanges.h>

#include <vector>


namespace DB
{

class ASTCreateShardQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String shard_name;
    std::vector<String> replicas;
    /// Parsed `PROPERTIES` list (syntax only at parse time). Semantics validated in interpreter.
    SettingsChanges shard_properties;
    bool if_not_exists = false;
    /// After `ON CLUSTER ...`, optional `SYNC` (wait for distributed DDL when task timeout would otherwise skip it).
    bool sync = false;

    String getID(char) const override { return "CreateShardQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTCreateShardQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
