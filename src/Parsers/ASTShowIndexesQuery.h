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

    String getID(char) const override { return "ShowColumns"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}

