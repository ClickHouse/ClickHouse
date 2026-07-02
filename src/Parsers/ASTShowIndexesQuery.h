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

    /// The distinguishing fields — `extended`, the `database` / `table` and the `where_expression`
    /// clause — are plain members, not part of `children`, and `getID` is a constant. Fold them into
    /// the hash so the rewrite-rule matcher (which treats an equal tree hash as semantic equality)
    /// does not let a rule template for one `SHOW INDEXES` over-match an unrelated one. (`getID`
    /// previously returned `"ShowColumns"`, so a `SHOW INDEXES` rule could even collide with a
    /// `SHOW COLUMNS` query — that is fixed above.)
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}

