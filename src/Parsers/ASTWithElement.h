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
    bool name_is_double_quoted = false; /// true if the CTE name was written as `"X"` rather than X

    /** Get the text that identifies this element. */
    String getID(char) const override { return "WithElement"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
};

}
