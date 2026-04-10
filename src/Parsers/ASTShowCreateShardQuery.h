#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

class ASTShowCreateShardQuery : public ASTQueryWithOutput
{
public:
    String shard_name;

    String getID(char) const override { return "ShowCreateShardQuery"; }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
