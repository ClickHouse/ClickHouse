#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <vector>


namespace DB
{

class ASTCreateShardQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String shard_name;
    std::vector<String> replicas;
    UInt32 weight = 1;
    bool internal_replication = false;
    bool if_not_exists = false;

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
