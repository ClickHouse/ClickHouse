#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
/// SHOW [ROW] POLICIES [CURRENT] [ON [database.]table]
class ASTShowRowPoliciesQuery : public ASTQueryWithOutput
{
public:
    bool current = false;
    String database;
    String table_name;

    String getID(char) const override { return "SHOW POLICIES query"; }
    ASTPtr clone() const override { return std::make_shared<ASTShowRowPoliciesQuery>(*this); }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
