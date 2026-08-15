#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

/// Query SHOW INDEXES
class ASTShowIndexesQuery : public ASTQueryWithOutput
{
public:
    bool extended = false;

    ASTPtr where_expression;

    String database;
    String table;

    String getID(char) const override { return "ShowIndexes"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
