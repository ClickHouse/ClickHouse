#pragma once

#include <Core/IdentifierName.h>
#include <Parsers/IAST.h>


namespace DB
{
/** subquery in with statement
  */
class ASTWithElement : public IAST
{
public:
    String name;
    /// Quoting of the CTE name as written in the query. Double quotes pin the name to
    /// exact-case matching under `standard` name matching.
    IdentifierPartQuote name_quote = IdentifierPartQuote::Unquoted;
    ASTPtr subquery;
    ASTPtr aliases;

    bool is_materialized = false; /// WITH t AS MATERIALIZED (subquery)

    /** Get the text that identifies this element. */
    String getID(char) const override { return "WithElement"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
