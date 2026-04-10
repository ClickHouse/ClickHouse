#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

class ASTShowCreateClusterQuery : public ASTQueryWithOutput
{
public:
    String cluster_name;

    String getID(char) const override { return "ShowCreateClusterQuery"; }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
