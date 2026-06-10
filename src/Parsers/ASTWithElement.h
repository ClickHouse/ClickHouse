#pragma once

#include <Parsers/IAST.h>


namespace DB
{
/** subquery in with statement
  */
class ASTWithElement : public IAST
{
public:
    String name;
    ASTPtr subquery;
    ASTPtr aliases;

    bool is_materialized = false; /// WITH t AS MATERIALIZED (subquery)

    /// WITH RECURSIVE t USING KEY (a, b) AS (subquery) — keyed recursive CTE:
    /// the recursion accumulates one row per key (new rows replace same-key rows)
    /// and each step's working table contains only the changed rows.
    ASTPtr key_columns;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "WithElement"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
