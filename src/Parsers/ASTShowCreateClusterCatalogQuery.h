#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

/// `SHOW CREATE CLUSTER` / `SHOW CREATE SHARD` — render the DDL for a SQL cluster catalog entry.
class ASTShowCreateClusterCatalogQuery : public ASTQueryWithOutput
{
public:
    enum class Kind : uint8_t
    {
        Cluster,
        Shard,
    };

    Kind kind = Kind::Cluster;
    String name;

    String getID(char) const override
    {
        return kind == Kind::Cluster ? "ShowCreateClusterQuery" : "ShowCreateShardQuery";
    }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
